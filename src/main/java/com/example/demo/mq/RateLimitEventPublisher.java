package com.example.demo.mq;

import java.nio.charset.StandardCharsets;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Publishes {@link RateLimitEvent}s to the broker, without ever letting the broker affect
 * the caller.
 *
 * <p><strong>This class never throws and never blocks.</strong> Both properties are
 * deliberate and they are the whole point of it. {@code /check} has already decided and is
 * about to answer; a broker that is slow, full or simply not running must not turn a
 * correct 200 into a 500, and must not add its round trip to a latency budget the
 * synchronous Redis path was designed around. So the send is asynchronous
 * ({@code send} with a {@link SendCallback}), and everything -- serialisation failures,
 * client exceptions, a broker that rejects the message -- is logged and dropped.
 *
 * <p>The consequence is accepted openly: events are best-effort. Losing an audit line is
 * strictly better than failing the request that produced it, because the request is the
 * product and the audit line is a by-product. Anything that genuinely could not be lost
 * would need the transactional or at-least-once send that this design does not claim.
 *
 * <p>The tag is the event type, and RocketMQ filters tags <strong>on the broker</strong>:
 * a consumer that subscribes to {@code REQUEST_BLOCKED} alone never receives the
 * {@code REQUEST_ALLOWED} firehose at all, rather than pulling it down to discard it. That
 * is what lets both live on one topic. The message key is the API key, which is what makes
 * a message findable in the console when someone asks why one caller was throttled.
 */
public class RateLimitEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(RateLimitEventPublisher.class);

    private final DefaultMQProducer producer;
    private final ObjectMapper objectMapper;
    private final String topic;

    public RateLimitEventPublisher(DefaultMQProducer producer, ObjectMapper objectMapper, String topic) {
        this.producer = producer;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    /**
     * Sends one event, or gives up quietly.
     *
     * <p>The {@code catch} is on {@link Exception} rather than on the four checked types
     * the client declares, because the contract being kept is "the caller is unaffected",
     * and that has to hold for the unchecked ones too -- a serialisation error or a null
     * from a half-initialised producer would otherwise escape into the request path and
     * undo the entire arrangement above.
     */
    public void publish(RateLimitEvent event) {
        try {
            Message message = new Message(topic, event.eventType(),
                    objectMapper.writeValueAsString(event).getBytes(StandardCharsets.UTF_8));
            message.setKeys(event.apiKey());

            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.debug("Published {} for API key '{}' as message {}",
                            event.eventType(), event.apiKey(), sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable throwable) {
                    log.warn("Failed to publish {} for API key '{}'; the event is dropped",
                            event.eventType(), event.apiKey(), throwable);
                }
            });
        } catch (Exception e) {
            log.warn("Could not hand {} for API key '{}' to the producer; the event is dropped",
                    event.eventType(), event.apiKey(), e);
        }
    }
}
