package com.example.demo.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * The body of {@code POST /limits}: create a rule or overwrite an existing one.
 *
 * <p>{@code limit} on the wire, {@code limitCount} in Java. The split starts in the
 * database -- {@code limit} is a MySQL reserved word -- and stops here: the API is not
 * going to expose a column-naming workaround to its callers.
 *
 * <p>The two numbers are boxed rather than {@code int} so that omitting one is a
 * validation failure naming the missing field, instead of Jackson defaulting it to
 * {@code 0} and {@code @Min} reporting a value the caller never sent.
 *
 * <p>{@code apiKey} is capped at the width of the {@code api_key} column. Without the
 * cap an over-long key reaches MySQL and comes back as a data-truncation error -- a 500
 * for what is plainly a bad request.
 */
public record CreateLimitRequest(

        @NotBlank(message = "must not be blank")
        @Size(max = 128, message = "must be at most 128 characters")
        String apiKey,

        @JsonProperty("limit")
        @NotNull(message = "must not be null")
        @Min(value = 1, message = "must be at least 1")
        Integer limitCount,

        @NotNull(message = "must not be null")
        @Min(value = 1, message = "must be at least 1")
        Integer windowSeconds) {
}
