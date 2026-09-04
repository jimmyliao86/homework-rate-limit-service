package com.example.demo.controller;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.dto.CheckResponse;
import com.example.demo.dto.UsageResponse;
import com.example.demo.service.RateLimitCheckService;

import jakarta.validation.constraints.NotBlank;

import java.time.Instant;

/**
 * The two read-side endpoints: {@code GET /check}, which consumes quota, and
 * {@code GET /usage}, which does not.
 *
 * <p>Using {@code GET} for something that increments a counter is at odds with HTTP's
 * definition of a safe method -- a proxy or a browser prefetch is entitled to repeat it.
 * The brief specifies this interface, so it is implemented as asked and the conflict is
 * recorded here rather than quietly designed around.
 *
 * <p><strong>{@code @Validated} on the class is not optional.</strong> {@code apiKey}
 * arrives as a query parameter, and constraints on {@code @RequestParam} arguments are only
 * evaluated when method validation is switched on for the bean. Without this annotation
 * {@code @NotBlank} is inert, and {@code /check?apiKey=} would sail through to a Redis key
 * ending in a colon instead of being rejected with a 400.
 */
@RestController
@Validated
public class RateLimitCheckController {

    /**
     * Seconds until the current window resets. The value is a delta, not a Unix timestamp:
     * that is the {@code X-RateLimit-Reset} variant that needs no clock agreement between
     * server and client, and it is the same number the body reports as
     * {@code windowTtlSeconds}.
     */
    private static final String RESET_HEADER = "X-RateLimit-Reset";

    private static final String LIMIT_HEADER = "X-RateLimit-Limit";
    private static final String REMAINING_HEADER = "X-RateLimit-Remaining";

    private final RateLimitCheckService checkService;

    public RateLimitCheckController(RateLimitCheckService checkService) {
        this.checkService = checkService;
    }

    /**
     * Consumes one unit of quota and reports whether the caller may proceed.
     *
     * <p>A refusal is returned as {@code 429} carrying the ordinary {@link CheckResponse}
     * rather than as an exception, because "no" is a successful answer to the question this
     * endpoint asks. Routing it through {@code GlobalExceptionHandler} would replace the
     * body with a {@code ProblemDetail} and so withhold {@code remaining} and
     * {@code windowTtlSeconds} from the one caller who needs them most -- which is why no
     * rate-limit exception type exists anywhere in this codebase.
     */
    @GetMapping("/check")
    public ResponseEntity<CheckResponse> check(@RequestParam @NotBlank String apiKey) {
        CheckResponse response = checkService.check(apiKey);

        HttpHeaders headers = new HttpHeaders();
        headers.add(LIMIT_HEADER, String.valueOf(response.limitCount()));
        headers.add(REMAINING_HEADER, String.valueOf(response.remaining()));
        headers.add(RESET_HEADER, String.valueOf(Instant.now().getEpochSecond() + response.windowTtlSeconds()));
        if (!response.allowed()) {
            // The TTL is already in hand, so the standard 429 companion header costs
            // nothing and spares the client from parsing the body to find out how long to
            // back off for.
            headers.add(HttpHeaders.RETRY_AFTER, String.valueOf(response.windowTtlSeconds()));
        }

        HttpStatus status = response.allowed() ? HttpStatus.OK : HttpStatus.TOO_MANY_REQUESTS;
        return ResponseEntity.status(status).headers(headers).body(response);
    }

    /**
     * Reports the current window without consuming any of it.
     *
     * <p>No {@code X-RateLimit-*} headers here: those describe what just happened to a
     * rate-limited request, and nothing happened. Everything this endpoint knows is in the
     * body.
     */
    @GetMapping("/usage")
    public UsageResponse usage(@RequestParam @NotBlank String apiKey) {
        return checkService.usage(apiKey);
    }
}
