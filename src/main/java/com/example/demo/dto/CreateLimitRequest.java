package com.example.demo.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * The body of {@code POST /limits}: create a rule or overwrite an existing one.
 *
 * <p>The quota field is called {@code limit} here, not {@code limitCount} as everywhere
 * else in this codebase. That name exists because {@code limit} is a MySQL reserved word,
 * which constrains the table and the record that mirrors it -- but nothing on this class
 * ever reaches SQL, and Bean Validation reports the <em>Java</em> property path, which
 * {@code @JsonProperty} does not rename. Called {@code limitCount}, a rejected
 * {@code {"limit": 0}} would come back as {@code errors.limitCount}: a 400 that names a
 * field the caller never sent, defeating the point of the map.
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

        @NotNull(message = "must not be null")
        @Min(value = 1, message = "must be at least 1")
        Integer limit,

        @NotNull(message = "must not be null")
        @Min(value = 1, message = "must be at least 1")
        Integer windowSeconds) {
}
