package com.example.demo.config;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.demo.messaging.RateLimitEventConsumer;
import com.example.demo.messaging.RateLimitEventPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The RocketMQ client wiring: one producer, one push consumer, and the two classes that
 * use them.
 *
 * <p><strong>The whole class is conditional.</strong> Tests and demos must not need a
 * broker to start a Spring context, so {@code rocketmq.enabled=false} removes every bean
 * here at once. {@code matchIfMissing = true} keeps the normal case on by default -- the
 * switch is an escape hatch, not something the main profile has to remember to set.
 *
 * <p>That immediately creates the trap the switch exists to avoid: with the beans gone,
 * anything declaring a {@code RateLimitEventPublisher} dependency fails the context on a
 * missing bean, so the application would refuse to start in exactly the situation the flag
 * was added for. The two services therefore inject
 * {@code ObjectProvider<RateLimitEventPublisher>} and publish only if one is there.
 *
 * <p>The name server address is read with {@code @Value} because the native client, unlike
 * {@code rocketmq-spring-boot-starter}, brings no auto-configuration to bind it (§12.5).
 *
 * <p>Both clients are started and stopped by the container through
 * {@code initMethod}/{@code destroyMethod}, which for the consumer is not a convenience but
 * a correctness requirement: {@code subscribe} and {@code registerMessageListener} must
 * both have run before {@code start}, or the client throws. Doing the configuration in the
 * bean method body and leaving {@code start} to Spring is what guarantees that order.
 */
@Configuration
@ConditionalOnProperty(name = "rocketmq.enabled", havingValue = "true", matchIfMissing = true)
public class RocketMQConfig {

    /** Created on first send: the broker's {@code autoCreateTopicEnable} defaults to true. */
    public static final String TOPIC = "RATE_LIMIT_EVENTS";

    /**
     * Group names carry no wildcard: {@code DEFAULT_PRODUCER} and {@code DEFAULT_CONSUMER}
     * are reserved and the broker rejects them outright.
     */
    public static final String PRODUCER_GROUP = "RATE_LIMIT_PRODUCER";

    public static final String CONSUMER_GROUP = "RATE_LIMIT_AUDIT_CONSUMER";

    private final String nameServer;

    public RocketMQConfig(@Value("${rocketmq.name-server}") String nameServer) {
        this.nameServer = nameServer;
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public DefaultMQProducer rateLimitProducer() {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(nameServer);
        return producer;
    }

    @Bean
    public RateLimitEventPublisher rateLimitEventPublisher(DefaultMQProducer producer, ObjectMapper objectMapper) {
        return new RateLimitEventPublisher(producer, objectMapper, TOPIC);
    }

    @Bean
    public RateLimitEventConsumer rateLimitEventConsumer(ObjectMapper objectMapper) {
        return new RateLimitEventConsumer(objectMapper);
    }

    /**
     * Subscribes to every tag on the topic. The publisher tags each message with its event
     * type so a narrower consumer can be added later; the audit log wants all of them.
     *
     * <p>{@code CONSUME_FROM_FIRST_OFFSET} only applies the first time this consumer group
     * appears -- afterwards the broker remembers its offset. It means a demo run picks up
     * the events published before the consumer existed rather than appearing to do nothing.
     */
    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public DefaultMQPushConsumer rateLimitAuditConsumer(RateLimitEventConsumer listener) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(nameServer);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(TOPIC, "*");
        consumer.registerMessageListener(listener);
        return consumer;
    }
}
