package com.example.demo.exception;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Path;

/**
 * Turns every exception the API can raise into an RFC 7807 {@link ProblemDetail}, so all
 * endpoints fail in one shape.
 *
 * <p>It extends {@link ResponseEntityExceptionHandler} rather than standing alone because
 * Spring MVC raises a whole family of request-level exceptions this class would otherwise
 * never see -- malformed JSON, a missing query parameter, an unparseable {@code page}.
 * Left to the servlet container those come back as Boot's default error body, which is
 * not {@code problem+json}, and "the same shape everywhere" would quietly mean "except
 * for the errors we did not write ourselves".
 *
 * <p><strong>429 is deliberately not handled here.</strong> Being over the limit is a
 * successful answer to the question {@code /check} asks, and the caller needs
 * {@code remaining} and {@code windowTtlSeconds} precisely then -- which a
 * {@code ProblemDetail} would discard. The controller returns the ordinary response body
 * with a 429 status instead, so no exception type exists for it.
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * No rule for that API key: 404.
     *
     * <p>The key is echoed as an extension member as well as in the message, so a client
     * can react to it without parsing prose.
     */
    @ExceptionHandler(RuleNotFoundException.class)
    public ProblemDetail handleRuleNotFound(RuleNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setTitle("Rule Not Found");
        problem.setProperty("apiKey", ex.apiKey());
        return problem;
    }

    /**
     * A violated constraint on a query parameter (a blank {@code apiKey}, {@code size}
     * beyond its cap): 400.
     *
     * <p>Raised only for controllers annotated {@code @Validated} -- without it the
     * annotations on {@code @RequestParam} arguments are never evaluated at all.
     */
    @ExceptionHandler(ConstraintViolationException.class)
    public ProblemDetail handleConstraintViolation(ConstraintViolationException ex) {
        Map<String, String> errors = new LinkedHashMap<>();
        for (ConstraintViolation<?> violation : ex.getConstraintViolations()) {
            errors.put(parameterName(violation.getPropertyPath()), violation.getMessage());
        }
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "One or more request parameters are invalid.");
        problem.setTitle("Invalid Request");
        problem.setProperty("errors", errors);
        return problem;
    }

    /**
     * Redis is unreachable or too slow: 503.
     *
     * <p>Fail closed. The counters live only in Redis -- MySQL stores {@code limit = 100},
     * never "73 used" -- so there is nothing to fall back to, and a rate limiter that
     * opens the gates the moment it loses sight of the count removes the only protection
     * downstream has at the worst possible moment.
     *
     * <p>{@link QueryTimeoutException} covers the config cache's own load timeout as well
     * as a Redis command timeout; both mean the same thing to a caller -- try again
     * shortly.
     */
    @ExceptionHandler({RedisConnectionFailureException.class, QueryTimeoutException.class})
    public ProblemDetail handleBackendUnavailable(Exception ex) {
        log.error("Rate limit backend unavailable", ex);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "The rate limit backend is temporarily unavailable. Please retry shortly.");
        problem.setTitle("Service Unavailable");
        return problem;
    }

    /**
     * Anything unforeseen: 500, logged in full and reported in outline.
     *
     * <p>The exception message is kept out of the response on purpose: it is written for
     * whoever reads the logs, and it regularly carries a SQL statement, a host name or an
     * internal class name that no caller should be handed.
     */
    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex) {
        log.error("Unhandled exception", ex);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR,
                "An unexpected error occurred while processing the request.");
        problem.setTitle("Internal Server Error");
        return problem;
    }

    /**
     * A rejected {@code @Valid @RequestBody}: 400.
     *
     * <p>Overridden rather than declared fresh so the framework's status and headers are
     * kept; only the body gains the field-by-field breakdown, without which the caller is
     * told that the request is invalid but not which part of it.
     */
    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request) {

        Map<String, String> errors = new LinkedHashMap<>();
        for (FieldError fieldError : ex.getBindingResult().getFieldErrors()) {
            errors.put(fieldError.getField(), messageOf(fieldError.getDefaultMessage()));
        }
        ex.getBindingResult().getGlobalErrors()
                .forEach(error -> errors.put(error.getObjectName(), messageOf(error.getDefaultMessage())));

        ProblemDetail problem = ex.getBody();
        problem.setTitle("Invalid Request");
        problem.setProperty("errors", errors);
        return handleExceptionInternal(ex, problem, headers, status, request);
    }

    /**
     * Method validation reports a path of {@code method.parameter}; only the trailing node
     * names anything the caller sent, so reporting {@code apiKey} rather than
     * {@code check.apiKey} keeps the response about the request instead of about our
     * method signatures.
     */
    private static String parameterName(Path path) {
        String name = null;
        for (Path.Node node : path) {
            name = node.getName();
        }
        return name != null ? name : path.toString();
    }

    private static String messageOf(String defaultMessage) {
        return defaultMessage != null ? defaultMessage : "invalid value";
    }
}
