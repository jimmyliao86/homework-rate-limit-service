package com.example.demo.service;

/**
 * The Redis key layout, in one place.
 *
 * <p>Three call sites share it -- the config cache, the rate limit check and the delete
 * flow -- and the delete flow only works if it builds byte-for-byte the same keys the
 * other two wrote. A format string copied into three classes is a silent leak waiting to
 * happen: the copies drift, {@code DELETE /limits/{apiKey}} removes nothing, and the rule
 * keeps being enforced after it is gone.
 */
public final class RedisKeys {

    private static final String CONFIG_PREFIX = "rate_limit:config:";
    private static final String COUNTER_PREFIX = "rate_limit:counter:";

    private RedisKeys() {
    }

    /** The cached rule, or the negative-cache tombstone. */
    public static String config(String apiKey) {
        return CONFIG_PREFIX + apiKey;
    }

    /**
     * The counter for one window of one version of a rule.
     *
     * <p>The version is part of the key rather than a field inside it, so changing a rule
     * silently retires the old counter: nothing reads it again and its own TTL removes it.
     * That is why no cleanup job exists -- and why the version has to be known before the
     * key can be deleted, which is what forces the delete flow to read the rule first.
     */
    public static String counter(String apiKey, long version) {
        return COUNTER_PREFIX + apiKey + ":v" + version;
    }
}
