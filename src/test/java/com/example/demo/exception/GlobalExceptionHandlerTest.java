package com.example.demo.exception;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Valid;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

/**
 * Every exception-to-status mapping in {@link GlobalExceptionHandler}, driven through a
 * throwaway controller.
 *
 * <p>Standalone {@code MockMvc} rather than {@code @WebMvcTest}: the mappings must hold
 * for whatever the real controllers turn out to look like, so the test deliberately does
 * not depend on them -- and this way it costs no application context at all.
 */
class GlobalExceptionHandlerTest {

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(new ThrowingController())
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    @DisplayName("RuleNotFoundException -> 404 with the API key as an extension member")
    void ruleNotFoundIsNotFound() throws Exception {
        mockMvc.perform(get("/boom/rule-not-found"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.title").value("Rule Not Found"))
                .andExpect(jsonPath("$.detail").value(containsString("abc-123")))
                .andExpect(jsonPath("$.apiKey").value("abc-123"));
    }

    @Test
    @DisplayName("ConstraintViolationException -> 400 listing each parameter by its trailing path node")
    void constraintViolationIsBadRequest() throws Exception {
        mockMvc.perform(get("/boom/constraint-violation"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.title").value("Invalid Request"))
                .andExpect(jsonPath("$.errors.apiKey").exists())
                .andExpect(jsonPath("$.errors.limit").exists());
    }

    @Test
    @DisplayName("A rejected @Valid body -> 400 naming every offending field")
    void invalidBodyIsBadRequest() throws Exception {
        mockMvc.perform(post("/boom/body")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"apiKey\":\"  \",\"limit\":0}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.title").value("Invalid Request"))
                .andExpect(jsonPath("$.errors.apiKey").exists())
                .andExpect(jsonPath("$.errors.limit").exists());
    }

    @Test
    @DisplayName("Malformed JSON -> 400 in the same problem+json shape, not Boot's default error body")
    void malformedJsonIsBadRequest() throws Exception {
        mockMvc.perform(post("/boom/body")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"apiKey\":"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    @DisplayName("RedisConnectionFailureException -> 503, failing closed")
    void redisDownIsServiceUnavailable() throws Exception {
        mockMvc.perform(get("/boom/redis-down"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(503))
                .andExpect(jsonPath("$.title").value("Service Unavailable"));
    }

    @Test
    @DisplayName("QueryTimeoutException -> 503 as well, so a slow load reads like an unreachable one")
    void queryTimeoutIsServiceUnavailable() throws Exception {
        mockMvc.perform(get("/boom/query-timeout"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503));
    }

    @Test
    @DisplayName("Anything else -> 500 without leaking the exception message")
    void unexpectedExceptionIsInternalServerError() throws Exception {
        mockMvc.perform(get("/boom/unexpected"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(500))
                .andExpect(jsonPath("$.title").value("Internal Server Error"))
                .andExpect(jsonPath("$.detail").value(not(containsString("jdbc:mysql"))));
    }

    /**
     * A stand-in for the real controllers: each endpoint raises exactly one of the
     * exceptions the handler claims to map.
     */
    @RestController
    static class ThrowingController {

        @GetMapping("/boom/rule-not-found")
        void ruleNotFound() {
            throw new RuleNotFoundException("abc-123");
        }

        @GetMapping("/boom/constraint-violation")
        void constraintViolation() {
            throw new ConstraintViolationException(violate());
        }

        @GetMapping("/boom/redis-down")
        void redisDown() {
            throw new RedisConnectionFailureException("Unable to connect to Redis");
        }

        @GetMapping("/boom/query-timeout")
        void queryTimeout() {
            throw new QueryTimeoutException("Timed out loading the rule for API key 'abc-123'");
        }

        @GetMapping("/boom/unexpected")
        void unexpected() {
            throw new IllegalStateException("connection refused for jdbc:mysql://db-prod-01:3306/taskdb");
        }

        @PostMapping("/boom/body")
        void body(@Valid @RequestBody Payload payload) {
            // Never reached: every request this test sends fails validation first.
        }

        /**
         * Real violations from a real validator rather than stubs -- the handler reads the
         * property path, and a hand-built mock would be free to report a shape Hibernate
         * Validator never produces.
         */
        private static Set<ConstraintViolation<Payload>> violate() {
            try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
                return factory.getValidator().validate(new Payload("  ", 0));
            }
        }
    }

    record Payload(@NotBlank String apiKey, @Min(1) int limit) {
    }
}
