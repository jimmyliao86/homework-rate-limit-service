package com.example.demo.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.example.demo.mq.RateLimitEventConsumer;
import com.example.demo.mq.RateLimitEventPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The MQ wiring, examined without a broker to talk to.
 *
 * <p>The bean methods are called directly for the consumer assertions. Letting Spring build
 * them would mean {@code initMethod = "start"}, and starting either client without a name
 * server is not something a unit test should depend on -- while what is actually worth
 * checking is precisely the state the bean is in <em>before</em> {@code start} runs.
 */
class RocketMQConfigTest {

    private static final String NAME_SERVER = "localhost:9876";

    private final RocketMQConfig config = new RocketMQConfig(NAME_SERVER);

    @Test
    @DisplayName("rocketmq.enabled=false removes every MQ bean and the context still starts")
    void theSwitchRemovesEveryBean() {
        new ApplicationContextRunner()
                .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations.of(JacksonAutoConfiguration.class))
                .withUserConfiguration(RocketMQConfig.class)
                .withPropertyValues("rocketmq.enabled=false", "rocketmq.name-server=" + NAME_SERVER)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    // The publisher's absence is the whole point of the switch -- and the
                    // reason both services reach it through an ObjectProvider.
                    assertThat(context).doesNotHaveBean(RateLimitEventPublisher.class);
                    assertThat(context).doesNotHaveBean(RateLimitEventConsumer.class);
                    assertThat(context).doesNotHaveBean(DefaultMQProducer.class);
                    assertThat(context).doesNotHaveBean(DefaultMQPushConsumer.class);
                });
    }

    @Test
    @DisplayName("The producer uses a group name of its own, not the reserved default")
    void configuresTheProducer() {
        DefaultMQProducer producer = config.rateLimitProducer();

        assertThat(producer.getProducerGroup()).isEqualTo("RATE_LIMIT_PRODUCER");
        assertThat(producer.getProducerGroup()).isNotEqualTo("DEFAULT_PRODUCER");
        assertThat(producer.getNamesrvAddr()).isEqualTo(NAME_SERVER);
    }

    @Test
    @DisplayName("The consumer is subscribed and has its listener before start() could run")
    void configuresTheConsumerBeforeItIsStarted() throws Exception {
        RateLimitEventConsumer listener = config.rateLimitEventConsumer(new ObjectMapper());

        DefaultMQPushConsumer consumer = config.rateLimitAuditConsumer(listener);

        // subscribe() and registerMessageListener() must both have happened by the time the
        // bean method returns, because Spring calls start() straight afterwards and start()
        // throws MQClientException if either is still missing.
        // Read through the impl, not getSubscription(): that map is the deprecated
        // setSubscription() copy and stays empty, while subscribe() writes here -- which is
        // also where start() looks.
        assertThat(consumer.getDefaultMQPushConsumerImpl().getSubscriptionInner())
                .hasEntrySatisfying(RocketMQConfig.TOPIC, data -> assertThat(data.getSubString()).isEqualTo("*"));
        assertThat(consumer.getMessageListener()).isSameAs(listener);
        assertThat(consumer.getConsumerGroup()).isEqualTo("RATE_LIMIT_AUDIT_CONSUMER");
        assertThat(consumer.getNamesrvAddr()).isEqualTo(NAME_SERVER);
    }
}
