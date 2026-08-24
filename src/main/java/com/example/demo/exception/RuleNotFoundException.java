package com.example.demo.exception;

/**
 * No rate limit rule exists for the requested API key.
 *
 * <p>Raised by the config cache -- either after MySQL returned nothing, or straight from
 * the negative-cache tombstone without touching MySQL at all. The
 * {@code GlobalExceptionHandler} maps it to {@code 404}.
 */
public class RuleNotFoundException extends RuntimeException {

    private final transient String apiKey;

    public RuleNotFoundException(String apiKey) {
        super("No rate limit rule found for API key '" + apiKey + "'");
        this.apiKey = apiKey;
    }

    public String apiKey() {
        return apiKey;
    }
}
