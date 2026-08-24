package com.example.demo.controller;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.example.demo.dto.CheckResponse;
import com.example.demo.dto.UsageResponse;
import com.example.demo.exception.RuleNotFoundException;
import com.example.demo.service.RateLimitCheckService;

/**
 * The web contract of {@code /check} and {@code /usage}: status codes, body shape and
 * headers, with the service mocked so only the HTTP layer is under test.
 *
 * <p>The 404 and 503 mappings are asserted here as well as in the handler's own test, and
 * that is not duplication: the handler test builds its advice by hand, so it can prove the
 * mapping is correct but not that the advice is picked up alongside this controller. Only
 * a request through the real context answers that.
 */
@WebMvcTest(RateLimitCheckController.class)
class RateLimitCheckControllerTest {

    private static final String API_KEY = "abc-123";

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private RateLimitCheckService checkService;

    @Test
    @DisplayName("Within the limit -> 200 with the window state in both the body and the X-RateLimit-* headers")
    void allowedRequestIsOk() throws Exception {
        given(checkService.check(API_KEY))
                .willReturn(new CheckResponse(API_KEY, true, 73, 100, 27, 42, 7));

        mockMvc.perform(get("/check").param("apiKey", API_KEY))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.apiKey").value(API_KEY))
                .andExpect(jsonPath("$.allowed").value(true))
                .andExpect(jsonPath("$.usage").value(73))
                .andExpect(jsonPath("$.limit").value(100))
                .andExpect(jsonPath("$.remaining").value(27))
                .andExpect(jsonPath("$.windowTtlSeconds").value(42))
                .andExpect(jsonPath("$.version").value(7))
                .andExpect(header().string("X-RateLimit-Limit", "100"))
                .andExpect(header().string("X-RateLimit-Remaining", "27"))
                .andExpect(header().string("X-RateLimit-Reset", "42"))
                // Retry-After belongs to a refusal; sending it on a success would tell a
                // healthy client to back off for no reason.
                .andExpect(header().doesNotExist(HttpHeaders.RETRY_AFTER));
    }

    @Test
    @DisplayName("Over the limit -> 429 with the very same body plus Retry-After, not a ProblemDetail")
    void blockedRequestIsTooManyRequests() throws Exception {
        given(checkService.check(API_KEY))
                .willReturn(new CheckResponse(API_KEY, false, 100, 100, 0, 17, 7));

        mockMvc.perform(get("/check").param("apiKey", API_KEY))
                .andExpect(status().isTooManyRequests())
                // The decisive assertion: application/json, not application/problem+json.
                // A throttled caller needs remaining and windowTtlSeconds, and RFC 7807
                // has nowhere to put them.
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.allowed").value(false))
                .andExpect(jsonPath("$.usage").value(100))
                .andExpect(jsonPath("$.limit").value(100))
                .andExpect(jsonPath("$.remaining").value(0))
                .andExpect(jsonPath("$.windowTtlSeconds").value(17))
                .andExpect(header().string(HttpHeaders.RETRY_AFTER, "17"))
                .andExpect(header().string("X-RateLimit-Limit", "100"))
                .andExpect(header().string("X-RateLimit-Remaining", "0"))
                .andExpect(header().string("X-RateLimit-Reset", "17"));
    }

    @Test
    @DisplayName("/usage reports the window without consuming any of it")
    void usageDoesNotIncrement() throws Exception {
        given(checkService.usage(API_KEY))
                .willReturn(new UsageResponse(API_KEY, 3, 3, 0, 55, 7));

        mockMvc.perform(get("/usage").param("apiKey", API_KEY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.apiKey").value(API_KEY))
                .andExpect(jsonPath("$.usage").value(3))
                .andExpect(jsonPath("$.limit").value(3))
                .andExpect(jsonPath("$.remaining").value(0))
                .andExpect(jsonPath("$.windowTtlSeconds").value(55))
                .andExpect(jsonPath("$.version").value(7))
                // No "allowed" field: answering it would mean spending quota to find out.
                .andExpect(jsonPath("$.allowed").doesNotExist());

        // The counter is only ever incremented by check(), so the endpoint reaching for it
        // at all would be the increment. peek.lua's read-only-ness is proven against a real
        // Redis in RateLimitScriptsTest; what is in question here is which of the two the
        // controller calls.
        then(checkService).should(never()).check(anyString());
    }

    @Test
    @DisplayName("No rule for the API key -> 404 problem+json")
    void missingRuleIsNotFound() throws Exception {
        given(checkService.check(API_KEY)).willThrow(new RuleNotFoundException(API_KEY));

        mockMvc.perform(get("/check").param("apiKey", API_KEY))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.apiKey").value(API_KEY));
    }

    @Test
    @DisplayName("Redis unreachable -> 503, failing closed rather than waving traffic through")
    void redisDownIsServiceUnavailable() throws Exception {
        given(checkService.check(API_KEY))
                .willThrow(new RedisConnectionFailureException("Unable to connect to Redis"));

        mockMvc.perform(get("/check").param("apiKey", API_KEY))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(503));
    }

    @Test
    @DisplayName("/usage is fail-closed too: a Redis timeout is a 503, not an empty snapshot")
    void usageWithRedisTimeoutIsServiceUnavailable() throws Exception {
        given(checkService.usage(API_KEY)).willThrow(new QueryTimeoutException("Redis command timed out"));

        mockMvc.perform(get("/usage").param("apiKey", API_KEY))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503));
    }

    @Test
    @DisplayName("A blank apiKey -> 400, which only happens because the controller is @Validated")
    void blankApiKeyIsBadRequest() throws Exception {
        mockMvc.perform(get("/check").param("apiKey", "   "))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.errors.apiKey").exists());

        // Drop @Validated and this request reaches the service with a blank key instead of
        // being rejected, so the absence of the call is what the test is really asserting.
        then(checkService).should(never()).check(anyString());
    }

    @Test
    @DisplayName("A missing apiKey -> 400 in the same problem+json shape")
    void missingApiKeyIsBadRequest() throws Exception {
        mockMvc.perform(get("/usage"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }
}
