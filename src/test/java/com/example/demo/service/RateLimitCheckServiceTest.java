package com.example.demo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
    private static final RateLimitConfig CONFIG = new RateLimitConfig(7, 100, 60);
    private static final String COUNTER_KEY = "rate_limit:counter:abc-123:v7";

    private final RedisConfig scripts = new RedisConfig();
    private final RedisScript<List> checkAndIncrScript = scripts.checkAndIncrScript();
    private final RedisScript<List> peekScript = scripts.peekScript();

    @Mock
    private RateLimitConfigCache configCache;

    @Mock
    private StringRedisTemplate redis;

    private RateLimitCheckService service;

    @BeforeEach
    void setUp() {
        service = new RateLimitCheckService(configCache, redis, checkAndIncrScript, peekScript);
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
