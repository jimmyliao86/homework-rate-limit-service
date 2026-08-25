package com.example.demo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

import java.util.List;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import com.example.demo.config.RedisConfig;
import com.example.demo.domain.RateLimitConfig;
import com.example.demo.dto.CheckResponse;
import com.example.demo.dto.UsageResponse;
import com.example.demo.exception.RuleNotFoundException;
import com.example.demo.messaging.RateLimitEvent;
import com.example.demo.messaging.RateLimitEventPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * What the service hands to Redis and what it makes of the reply.
 *
 * <p>The Lua behaviour itself is covered against a real container in
 * {@code RateLimitScriptsTest}; the failures this class exists to catch are on the Java
 * side of that boundary, and both are silent until runtime: {@code ARGV} that is not a
 * {@code String} never reaches Lua as a number, and a reply element read as
 * {@code Integer} throws where a {@code Number} would not.
 *
 * <p>The scripts come from a real {@link RedisConfig} rather than from mocks so that the
 * two are genuinely distinct objects loaded from their own files -- which is what lets the
 * assertion "{@code /usage} ran the read-only script" mean anything at all.
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings("rawtypes")
class RateLimitCheckServiceTest {

    private static final String API_KEY = "abc-123";
    private static final long CREATED_AT_MS = 1787670000000L;
    private static final RateLimitConfig CONFIG = new RateLimitConfig(CREATED_AT_MS, 7, 100, 60);

    /**
     * Spelled out rather than built through {@link RedisKeys}, so that a change to the key
     * layout has to be made deliberately in both places instead of silently agreeing with
     * itself. Both discriminators are present: the incarnation and the version.
     */
    private static final String COUNTER_KEY = "rate_limit:counter:abc-123:c" + CREATED_AT_MS + ":v7";

    private final RedisConfig scripts = new RedisConfig();
    private final RedisScript<List> checkAndIncrScript = scripts.checkAndIncrScript();
    private final RedisScript<List> peekScript = scripts.peekScript();

    @Mock
    private RateLimitConfigCache configCache;

    @Mock
    private StringRedisTemplate redis;

    @Mock
    private RateLimitEventPublisher publisher;

    @Captor
    private ArgumentCaptor<RateLimitEvent> event;

    private RateLimitCheckService service;

    @BeforeEach
    void setUp() {
        service = new RateLimitCheckService(configCache, redis, checkAndIncrScript, peekScript,
                TestObjectProvider.of(publisher));
    }

    @Test
    @DisplayName("/check runs the incrementing script on the versioned counter key with string arguments")
    void checkSendsStringArgumentsToTheVersionedCounter() {
        given(configCache.get(API_KEY)).willReturn(CONFIG);
        given(redis.execute(same(checkAndIncrScript), anyList(), any(Object[].class)))
                .willReturn(List.of(1L, 73L, 42L));

        CheckResponse response = service.check(API_KEY);

        // The argument matchers are the assertion: eq("100") fails against Integer 100.
        // The arguments are serialised with the template's value serializer, and a
        // StringRedisTemplate can only write what is already a String.
        then(redis).should().execute(same(checkAndIncrScript), eq(List.of(COUNTER_KEY)), eq("100"), eq("60"));

        assertThat(response)
                .isEqualTo(new CheckResponse(API_KEY, true, 73, 100, 27, 42, 7));
    }

    @Test
    @DisplayName("A refusal from the script becomes allowed=false with remaining 0, never a negative")
    void blockedRequestReportsNoRemainingQuota() {
        given(configCache.get(API_KEY)).willReturn(CONFIG);
        given(redis.execute(same(checkAndIncrScript), anyList(), any(Object[].class)))
                .willReturn(List.of(0L, 100L, 17L));

        CheckResponse response = service.check(API_KEY);

        assertThat(response.allowed()).isFalse();
        assertThat(response.usage()).isEqualTo(100);
        assertThat(response.remaining()).isZero();
        assertThat(response.windowTtlSeconds()).isEqualTo(17);
    }

    @Test
    @DisplayName("/usage runs the read-only script and passes it no arguments at all")
    void usageRunsThePeekScriptOnly() {
        given(configCache.get(API_KEY)).willReturn(CONFIG);
        given(redis.execute(same(peekScript), anyList(), any(Object[].class)))
                .willReturn(List.of(73L, 42L));

        UsageResponse response = service.usage(API_KEY);

        // No trailing matchers: peek.lua takes no ARGV, and this verification only passes
        // against a call that supplied none.
        then(redis).should().execute(same(peekScript), eq(List.of(COUNTER_KEY)));
        then(redis).should(never()).execute(same(checkAndIncrScript), anyList(), any(Object[].class));

        assertThat(response).isEqualTo(new UsageResponse(API_KEY, 73, 100, 27, 42, 7));
    }

    @Test
    @DisplayName("A blocked request publishes REQUEST_BLOCKED describing the window that refused it")
    void blockedRequestPublishesAnEvent() {
        given(configCache.get(API_KEY)).willReturn(CONFIG);
        given(redis.execute(same(checkAndIncrScript), anyList(), any(Object[].class)))
                .willReturn(List.of(0L, 100L, 17L));

        service.check(API_KEY);

        then(publisher).should().publish(event.capture());
        assertThat(event.getValue().eventType()).isEqualTo(RateLimitEvent.REQUEST_BLOCKED);
        assertThat(event.getValue().apiKey()).isEqualTo(API_KEY);
        assertThat(event.getValue().version()).isEqualTo(7);
        assertThat(event.getValue().usage()).isEqualTo(100);
        assertThat(event.getValue().limitCount()).isEqualTo(100);
        assertThat(event.getValue().windowSeconds()).isEqualTo(60);
    }

    @Test
    @DisplayName("An allowed request publishes REQUEST_ALLOWED without changing the response")
    void allowedRequestPublishesTheServedOutcome() {
        given(configCache.get(API_KEY)).willReturn(CONFIG);
        given(redis.execute(same(checkAndIncrScript), anyList(), any(Object[].class)))
                .willReturn(List.of(1L, 73L, 42L));

        CheckResponse response = service.check(API_KEY);

        then(publisher).should().publish(event.capture());
        assertThat(event.getValue().eventType()).isEqualTo(RateLimitEvent.REQUEST_ALLOWED);
        assertThat(event.getValue().apiKey()).isEqualTo(API_KEY);
        assertThat(event.getValue().version()).isEqualTo(7);
        assertThat(event.getValue().usage()).isEqualTo(73);
        assertThat(event.getValue().limitCount()).isEqualTo(100);
        assertThat(event.getValue().windowSeconds()).isEqualTo(60);

        // Publishing is an observation of the decision, never part of making it: this is the
        // same body the service returned before the allowed path published anything.
        assertThat(response).isEqualTo(new CheckResponse(API_KEY, true, 73, 100, 27, 42, 7));
    }

    @Test
    @DisplayName("A broker failure on the allowed path leaves the response untouched")
    void brokerFailureOnTheAllowedPathDoesNotAffectTheResponse() throws Exception {
        // A real publisher over a producer that refuses to send, rather than a mock that is
        // told not to throw. The claim being tested spans both classes: /check survives a
        // broken broker only because the publisher swallows the failure, and the allowed
        // path is now where nearly all of that traffic goes.
        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        willThrow(new MQClientException("broker unreachable", null))
                .given(producer).send(any(Message.class), any(SendCallback.class));
        RateLimitCheckService withRealPublisher = new RateLimitCheckService(configCache, redis,
                checkAndIncrScript, peekScript,
                TestObjectProvider.of(new RateLimitEventPublisher(producer, new ObjectMapper(), "RATE_LIMIT_EVENTS")));

        given(configCache.get(API_KEY)).willReturn(CONFIG);
        given(redis.execute(same(checkAndIncrScript), anyList(), any(Object[].class)))
                .willReturn(List.of(1L, 73L, 42L));

        assertThat(withRealPublisher.check(API_KEY))
                .isEqualTo(new CheckResponse(API_KEY, true, 73, 100, 27, 42, 7));
    }

    @Test
    @DisplayName("/usage publishes nothing either -- it observes the window, it does not refuse anything")
    void usagePublishesNothing() {
        given(configCache.get(API_KEY)).willReturn(CONFIG);
        given(redis.execute(same(peekScript), anyList(), any(Object[].class)))
                .willReturn(List.of(100L, 17L));

        service.usage(API_KEY);

        then(publisher).shouldHaveNoInteractions();
    }

    @Test
    @DisplayName("With MQ switched off a blocked request is still answered normally")
    void blockedRequestWorksWithoutAPublisher() {
        RateLimitCheckService withoutMq = new RateLimitCheckService(configCache, redis,
                checkAndIncrScript, peekScript, TestObjectProvider.empty());
        given(configCache.get(API_KEY)).willReturn(CONFIG);
        given(redis.execute(same(checkAndIncrScript), anyList(), any(Object[].class)))
                .willReturn(List.of(0L, 100L, 17L));

        // The absent publisher is what rocketmq.enabled=false leaves behind; reaching for it
        // regardless would turn every 429 into a 500 in exactly that configuration.
        CheckResponse response = withoutMq.check(API_KEY);

        assertThat(response.allowed()).isFalse();
        then(publisher).shouldHaveNoInteractions();
    }

    @Test
    @DisplayName("A missing rule short-circuits before Redis is touched")
    void missingRuleNeverReachesTheCounter() {
        given(configCache.get(API_KEY)).willThrow(new RuleNotFoundException(API_KEY));

        assertThatThrownBy(() -> service.check(API_KEY)).isInstanceOf(RuleNotFoundException.class);

        then(redis).shouldHaveNoInteractions();
    }

    @Test
    @DisplayName("A Redis failure propagates untouched, so the handler can fail closed with 503")
    void redisFailurePropagates() {
        given(configCache.get(API_KEY)).willReturn(CONFIG);
        given(redis.execute(same(checkAndIncrScript), anyList(), any(Object[].class)))
                .willThrow(new RedisConnectionFailureException("Unable to connect to Redis"));

        // Swallowing this and serving a permissive answer would let every request through
        // at exactly the moment the limiter stopped working.
        assertThatThrownBy(() -> service.check(API_KEY))
                .isInstanceOf(RedisConnectionFailureException.class);
    }

    @Test
    @DisplayName("A null reply from Redis fails loudly instead of a silent NullPointerException")
    void nullReplyFromRedisFailsWithAClearMessage() {
        given(configCache.get(API_KEY)).willReturn(CONFIG);
        given(redis.execute(same(checkAndIncrScript), anyList(), any(Object[].class)))
                .willReturn(null);

        assertThatThrownBy(() -> service.check(API_KEY))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(API_KEY);
    }
}
