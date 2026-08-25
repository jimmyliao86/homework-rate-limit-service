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
    private static final String EPOCH_PREFIX = "rate_limit:epoch:";

    private RedisKeys() {
    }

    /** The cached rule, or the negative-cache tombstone. */
    public static String config(String apiKey) {
        return CONFIG_PREFIX + apiKey;
    }

    /**
     * The write-back guard token for one rule: a UUID replaced on every write to it.
     *
     * <p>It answers one question for a reader that missed the cache -- <em>has this rule been
     * written since I looked?</em> -- so that a value read from MySQL before a concurrent
     * write cannot be cached after it. Nothing on the hot path reads this key; only the miss
     * path does.
     *
     * <p>Note it is the one key an invalidation <strong>writes</strong> rather than deletes.
     * Deleting it would hand a stale reader an accepted write-back, because an absent token
     * matches the empty sentinel that same reader may be holding.
     */
    public static String epoch(String apiKey) {
        return EPOCH_PREFIX + apiKey;
    }

    /**
     * The counter for one window, of one version, of one incarnation of a rule.
     *
     * <p>Both discriminators are load-bearing, and neither replaces the other.
     *
     * <p>{@code version} retires a counter when a rule is <em>updated</em>: it is part of the
     * key rather than a field inside it, so a rule change silently starts counting on a new
     * key while the old one is left for its own TTL. That is why no cleanup job exists.
     *
     * <p>{@code createdAtEpochMs} retires a counter when a rule is <em>deleted and created
     * again</em>, which {@code version} alone cannot do: it is only incremented by
     * {@code ON DUPLICATE KEY UPDATE}, so a plain insert after a delete resets it to 1 and
     * would re-address the previous incarnation's counters -- refusing the recreated rule's
     * very first request. {@code created_at} is exactly the discriminator needed, because
     * the upsert does not list it (so it survives every update) while a re-insert generates
     * a fresh one.
     *
     * <p>Both values have to be known before the key can be deleted, which is what forces
     * the delete flow to read the rule before removing it.
     */
    public static String counter(String apiKey, long createdAtEpochMs, long version) {
        return COUNTER_PREFIX + apiKey + ":c" + createdAtEpochMs + ":v" + version;
    }
}
