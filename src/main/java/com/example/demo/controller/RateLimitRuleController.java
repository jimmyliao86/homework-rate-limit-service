package com.example.demo.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.dto.CreateLimitRequest;
import com.example.demo.dto.LimitResponse;
import com.example.demo.dto.PagedResponse;
import com.example.demo.service.RateLimitRuleService;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/**
 * Rule management: {@code POST /limits}, {@code DELETE /limits/{apiKey}} and
 * {@code GET /limits}.
 *
 * <p>No {@code /api/v1} prefix -- the paths are the ones the brief names, and a version
 * segment nothing is going to serve a second version of is decoration.
 *
 * <p>The class is annotated {@code @Validated} because the constraints on
 * {@code @RequestParam} arguments are otherwise never evaluated: {@code @Valid} covers the
 * request body only. It makes {@code size=1000000} a {@code 400} instead of a query that
 * returns the whole table.
 */
@RestController
@RequestMapping("/limits")
@Validated
public class RateLimitRuleController {

    private static final String DEFAULT_PAGE = "0";
    private static final String DEFAULT_SIZE = "20";

    private final RateLimitRuleService service;

    public RateLimitRuleController(RateLimitRuleService service) {
        this.service = service;
    }

    /**
     * Upsert. {@code 201} when the rule is new, {@code 204} when an existing one was
     * overwritten -- and neither carries a body.
     *
     * <p>The status code already says which of the two happened, so a
     * {@code {"created": true}} body would only repeat the status line. Returning the
     * stored rule instead would mean reading back what the database just computed, which
     * §8 rejects for a reason that survives here: under read replicas that read can lag and
     * report the previous version.
     */
    @PostMapping
    public ResponseEntity<Void> create(@Valid @RequestBody CreateLimitRequest request) {
        boolean created = service.save(request);
        return created
                ? ResponseEntity.status(HttpStatus.CREATED).build()
                : ResponseEntity.noContent().build();
    }

    /**
     * Removes the rule and the Redis state derived from it; {@code 404} if there is no
     * such rule.
     */
    @DeleteMapping("/{apiKey}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String apiKey) {
        service.delete(apiKey);
    }

    /**
     * Lists rules newest first, one page at a time.
     *
     * <p>{@code size} is capped at 100. The cap is the point of the parameter being
     * validated at all: pagination that a caller can opt out of by asking for a large
     * enough page protects nothing, and the response would be built by loading every rule
     * in the table into memory at once.
     */
    @GetMapping
    public PagedResponse<LimitResponse> list(
            @RequestParam(defaultValue = DEFAULT_PAGE) @Min(value = 0, message = "must be at least 0") int page,
            @RequestParam(defaultValue = DEFAULT_SIZE) @Min(value = 1, message = "must be at least 1")
            @Max(value = 100, message = "must be at most 100") int size) {

        return service.list(page, size);
    }
}
