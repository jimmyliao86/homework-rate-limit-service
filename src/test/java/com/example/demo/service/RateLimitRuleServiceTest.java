package com.example.demo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.demo.dto.CreateLimitRequest;
import com.example.demo.dto.LimitResponse;
import com.example.demo.dto.PagedResponse;
import com.example.demo.exception.RuleNotFoundException;
import com.example.demo.model.RateLimitRule;
import com.example.demo.mq.RateLimitEvent;
import com.example.demo.mq.RateLimitEventPublisher;
import com.example.demo.repository.RateLimitRuleRepository;

/**
 * The rule service against mocked collaborators.
 *
 * <p>Everything worth asserting here is about <em>order</em> and <em>how often</em> --
 * which is what a mock states directly and a real database and Redis can only be
 * interrogated about after the fact. The delete flow in particular is correct only in one
 * order, and that ordering is invisible to a test that just checks the row and the keys
 * are gone at the end.
 */
@ExtendWith(MockitoExtension.class)
class RateLimitRuleServiceTest {

    private static final String API_KEY = "abc-123";
    private static final long VERSION = 7;
    private static final OffsetDateTime CREATED_AT = OffsetDateTime.parse("2026-08-23T23:00:00+08:00");
    private static final long CREATED_AT_MS = CREATED_AT.toInstant().toEpochMilli();

    /** The counter key the delete flow has to name exactly, wildcards being off the table. */
    private static final String COUNTER_KEY = RedisKeys.counter(API_KEY, CREATED_AT_MS, VERSION);

    private static final RateLimitRule RULE = new RateLimitRule(
            API_KEY, 100, 60, VERSION,
            CREATED_AT,
            OffsetDateTime.parse("2026-08-24T09:30:00+08:00"));

    @Mock
    private RateLimitRuleRepository repository;

    @Mock
    private RateLimitConfigCache cache;

    @Mock
    private RateLimitEventPublisher publisher;

    @Captor
    private ArgumentCaptor<RateLimitEvent> event;

    private RateLimitRuleService service;

    @BeforeEach
    void setUp() {
        service = new RateLimitRuleService(repository, cache, TestObjectProvider.of(publisher));
    }

    @Test
    @DisplayName("DELETE clears Redis, deletes the row, then clears Redis again")
    void deleteFollowsTheThreePhaseOrdering() {
        given(repository.findByApiKey(API_KEY)).willReturn(Optional.of(RULE));
        given(repository.deleteByApiKey(API_KEY)).willReturn(1);

        service.delete(API_KEY);

        InOrder inOrder = inOrder(repository, cache);
        // The version has to be known before the counter key can be named at all.
        inOrder.verify(repository).findByApiKey(API_KEY);
        // Derived state first: if the row delete then fails, the next /check rebuilds the
        // cache from MySQL. The reverse order would leave a deleted rule being enforced.
        inOrder.verify(cache).invalidate(API_KEY, COUNTER_KEY);
        inOrder.verify(repository).deleteByApiKey(API_KEY);
        // Again, because a concurrent /check can have repopulated the cache from the row
        // that was about to disappear.
        inOrder.verify(cache).invalidate(API_KEY, COUNTER_KEY);
        inOrder.verifyNoMoreInteractions();

        verify(cache, times(2)).invalidate(API_KEY, COUNTER_KEY);
    }

    @Test
    @DisplayName("DELETE deletes the counter by its exact versioned key, never a wildcard")
    void deleteTargetsTheVersionedCounterKey() {
        given(repository.findByApiKey(API_KEY)).willReturn(Optional.of(RULE));
        given(repository.deleteByApiKey(API_KEY)).willReturn(1);

        service.delete(API_KEY);

        // Both keys in one DEL, and the counter carries the version that was just read.
        verify(cache, times(2)).invalidate(API_KEY,
                "rate_limit:counter:abc-123:c" + CREATED_AT_MS + ":v7");
    }

    @Test
    @DisplayName("DELETE of an unknown API key is 404 and touches nothing")
    void deleteOfUnknownRuleThrowsWithoutSideEffects() {
        given(repository.findByApiKey(API_KEY)).willReturn(Optional.empty());

        assertThatExceptionOfType(RuleNotFoundException.class)
                .isThrownBy(() -> service.delete(API_KEY))
                .satisfies(ex -> assertThat(ex.apiKey()).isEqualTo(API_KEY));

        verify(repository, never()).deleteByApiKey(anyString());
        verifyNoInteractions(cache, publisher);
    }

    @Test
    @DisplayName("DELETE losing the race to a concurrent delete still clears Redis and does not fail")
    void deleteToleratesAConcurrentDelete() {
        given(repository.findByApiKey(API_KEY)).willReturn(Optional.of(RULE));
        given(repository.deleteByApiKey(API_KEY)).willReturn(0);

        service.delete(API_KEY);

        verify(cache, times(2)).invalidate(API_KEY, COUNTER_KEY);
    }

    @Test
    @DisplayName("An upsert that inserted reports created, so the controller can answer 201")
    void saveReportsCreatedForAnInsert() {
        given(repository.upsert(API_KEY, 100, 60)).willReturn(1);

        assertThat(service.save(new CreateLimitRequest(API_KEY, 100, 60))).isTrue();
    }

    @Test
    @DisplayName("An upsert that updated reports not created, so the controller can answer 204")
    void saveReportsUpdatedForAnUpdate() {
        given(repository.upsert(API_KEY, 50, 30)).willReturn(2);

        assertThat(service.save(new CreateLimitRequest(API_KEY, 50, 30))).isFalse();
    }

    @Test
    @DisplayName("POST invalidates before and after the write, mirroring DELETE")
    void saveInvalidatesAroundTheWrite() {
        given(repository.upsert(API_KEY, 100, 60)).willReturn(2);

        service.save(new CreateLimitRequest(API_KEY, 100, 60));

        // The call after the upsert is the load-bearing one: it replaces the guard token, and
        // a reader still holding the old token has then provably selected before the commit.
        // The call before covers a different failure -- an upsert that commits and then throws
        // would skip the second call entirely, leaving the pre-update rule cached for ten
        // minutes with nothing left to clear it.
        InOrder inOrder = inOrder(repository, cache);
        inOrder.verify(cache).invalidate(API_KEY);
        inOrder.verify(repository).upsert(API_KEY, 100, 60);
        inOrder.verify(cache).invalidate(API_KEY);

        verify(cache, times(2)).invalidate(API_KEY);
    }

    @Test
    @DisplayName("POST publishes RULE_UPDATED after the write")
    void savePublishesRuleUpdated() {
        given(repository.upsert(API_KEY, 100, 60)).willReturn(1);

        service.save(new CreateLimitRequest(API_KEY, 100, 60));

        InOrder inOrder = inOrder(repository, publisher);
        inOrder.verify(repository).upsert(API_KEY, 100, 60);
        inOrder.verify(publisher).publish(event.capture());

        assertThat(event.getValue().eventType()).isEqualTo(RateLimitEvent.RULE_UPDATED);
        assertThat(event.getValue().apiKey()).isEqualTo(API_KEY);
        assertThat(event.getValue().limitCount()).isEqualTo(100);
        assertThat(event.getValue().windowSeconds()).isEqualTo(60);
        // No version: save deliberately does not read the row back, so there is none to name.
        assertThat(event.getValue().version()).isEqualTo(RateLimitEvent.UNKNOWN);
    }

    @Test
    @DisplayName("DELETE publishes RULE_DELETED, with the version the row carried")
    void deletePublishesRuleDeleted() {
        given(repository.findByApiKey(API_KEY)).willReturn(Optional.of(RULE));
        given(repository.deleteByApiKey(API_KEY)).willReturn(1);

        service.delete(API_KEY);

        InOrder inOrder = inOrder(repository, publisher);
        // Announced only once the row is actually gone.
        inOrder.verify(repository).deleteByApiKey(API_KEY);
        inOrder.verify(publisher).publish(event.capture());

        assertThat(event.getValue().eventType()).isEqualTo(RateLimitEvent.RULE_DELETED);
        assertThat(event.getValue().apiKey()).isEqualTo(API_KEY);
        assertThat(event.getValue().version()).isEqualTo(VERSION);
    }

    @Test
    @DisplayName("With MQ switched off the writes still succeed and publish nothing")
    void writesSucceedWithoutAPublisher() {
        RateLimitRuleService withoutMq =
                new RateLimitRuleService(repository, cache, TestObjectProvider.empty());
        given(repository.upsert(API_KEY, 100, 60)).willReturn(1);

        assertThat(withoutMq.save(new CreateLimitRequest(API_KEY, 100, 60))).isTrue();

        verifyNoInteractions(publisher);
    }

    @Test
    @DisplayName("GET /limits pages in the database and derives totalPages by ceiling division")
    void listPagesInTheDatabase() {
        given(repository.count()).willReturn(21L);
        given(repository.findPage(1, 20)).willReturn(List.of(RULE));

        PagedResponse<LimitResponse> response = service.list(1, 20);

        assertThat(response.page()).isEqualTo(1);
        assertThat(response.size()).isEqualTo(20);
        assertThat(response.totalElements()).isEqualTo(21);
        // 21 rules at 20 per page is 2 pages, not 1.
        assertThat(response.totalPages()).isEqualTo(2);
        assertThat(response.content()).containsExactly(LimitResponse.from(RULE));
    }

    @Test
    @DisplayName("GET /limits on an empty table reports zero pages rather than one empty one")
    void listOfAnEmptyTableHasNoPages() {
        given(repository.count()).willReturn(0L);
        given(repository.findPage(0, 20)).willReturn(List.of());

        PagedResponse<LimitResponse> response = service.list(0, 20);

        assertThat(response.content()).isEmpty();
        assertThat(response.totalElements()).isZero();
        assertThat(response.totalPages()).isZero();
    }
}
