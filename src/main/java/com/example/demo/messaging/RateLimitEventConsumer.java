package com.example.demo.messaging;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The audit consumer: it writes each event to the log and acknowledges it.
 *
 * <p>Logging is the entire behaviour on purpose. The consumer exists to show the chain
 * producer -> broker -> consumer actually closing, and to keep the queue draining so
 * messages do not pile up behind a topic nobody reads. Analytics, alerting and audit
 * storage are exactly the things this architecture makes possible <em>later</em> without
 * touching the request path -- they are named in the design and left unbuilt.
 *
 * <p><strong>The level is chosen per event type</strong>, because one line per event at a
 * single level stopped being viable once every {@code /check} publishes:
 *
 * <ul>
 * <li>{@code REQUEST_BLOCKED} at {@code INFO} -- the anomaly, comparatively rare, and
 *     actionable on its own because the line carries the API key and the window.</li>
 * <li>{@code REQUEST_ALLOWED} at {@code DEBUG} -- ordinary traffic, wanted while developing
 *     and silent in production.</li>
 * <li>{@code RULE_UPDATED} and {@code RULE_DELETED} at {@code INFO} -- rare, and each one
 *     matters.</li>
 * </ul>
 *
 * <p><strong>No counters, no aggregation, no scheduled flush.</strong> Tallying events here
 * to emit a periodic block rate is the obvious next step and it is deliberately not taken.
 * A rate averaged across all keys is close to useless -- rate limiting is per-key, so
 * ninety-nine healthy keys plus one being hammered to a 100% block rate still reads as a
 * comfortable ~1% overall, hiding precisely the situation worth seeing. Computing rates is
 * what the downstream consumers this topic exists for are supposed to do; doing it here
 * turns a demonstration consumer into a half-finished metrics system, and a stateful one at
 * that, in a listener the broker may invoke from several threads at once.
 *
 * <p><strong>A message that cannot be parsed is still acknowledged.</strong>
 * {@code RECONSUME_LATER} would be the reflex, but it is wrong here: a malformed body will
 * be just as malformed on the sixteenth attempt, so the only thing redelivery buys is the
 * same failure sixteen more times before the message lands in the dead-letter topic
 * regardless. Logging the raw body preserves everything a human needs, which for a
 * log-only consumer is the whole value of the message.
 */
public class RateLimitEventConsumer implements MessageListenerConcurrently {

    private static final Logger log = LoggerFactory.getLogger(RateLimitEventConsumer.class);

    private final ObjectMapper objectMapper;

    public RateLimitEventConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    private static final String AUDIT_LINE =
            "Audit event={} eventId={} apiKey={} version={} usage={} limit={} windowSeconds={} occurredAtEpochMs={}";

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
        for (MessageExt message : messages) {
            String body = new String(message.getBody(), StandardCharsets.UTF_8);
            try {
                RateLimitEvent event = objectMapper.readValue(body, RateLimitEvent.class);
                // The fluent builder picks the level without duplicating the eight-field
                // line, and resolves to a no-op builder when that level is disabled -- so a
                // production run with DEBUG off does no formatting work for the events that
                // make up nearly all the volume.
                (isRoutineTraffic(event) ? log.atDebug() : log.atInfo())
                        .log(AUDIT_LINE, event.eventType(), event.eventId(), event.apiKey(),
                                event.version(), event.usage(), event.limitCount(),
                                event.windowSeconds(), event.occurredAtEpochMs());
            } catch (Exception e) {
                log.warn("Discarding unreadable message {}: {}", message.getMsgId(), body, e);
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    /**
     * Everything except a served request is worth a line by default.
     *
     * <p>Written as "is this the routine case" rather than as a list of the interesting
     * ones, so an event type added later is loud until someone decides otherwise -- the
     * failure mode of a new event nobody sees is worse than one line too many.
     */
    private static boolean isRoutineTraffic(RateLimitEvent event) {
        return RateLimitEvent.REQUEST_ALLOWED.equals(event.eventType());
    }
}
