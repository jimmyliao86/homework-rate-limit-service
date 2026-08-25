package com.example.demo.mq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

/**
 * The audit consumer, driven directly rather than through a broker.
 *
 * <p>The log is the consumer's only output, so it is also the only thing worth asserting --
 * hence a real appender attached to the real logger rather than a mocked one. What matters
 * is the routing: with every {@code /check} publishing, a consumer that logged served
 * requests at {@code INFO} would drown the refusals in exactly the traffic they are
 * supposed to stand out from.
 */
class RateLimitEventConsumerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RateLimitEventConsumer consumer = new RateLimitEventConsumer(objectMapper);

    private final Logger logger = (Logger) LoggerFactory.getLogger(RateLimitEventConsumer.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    @BeforeEach
    void attachAppender() {
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        logger.detachAppender(appender);
        appender.stop();
        // Back to whatever the configuration says, so the level set by one test cannot
        // decide what another one observes.
        logger.setLevel(null);
    }

    @Test
    @DisplayName("A mixed batch is acknowledged and each event lands at its intended level")
    void routesEachEventTypeToItsOwnLevel() throws Exception {
        logger.setLevel(Level.DEBUG);

        ConsumeConcurrentlyStatus status = consumer.consumeMessage(List.of(
                message(RateLimitEvent.requestAllowed("abc-123", 7, 73, 100, 60)),
                message(RateLimitEvent.requestBlocked("abc-123", 7, 100, 100, 60)),
                message(RateLimitEvent.ruleUpdated("abc-123", 100, 60)),
                message(RateLimitEvent.ruleDeleted("abc-123", 7, 100, 60))), null);

        assertThat(status).isEqualTo(ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        assertThat(appender.list)
                .extracting(ILoggingEvent::getLevel, event -> event.getArgumentArray()[0])
                .containsExactly(
                        tuple(Level.DEBUG, RateLimitEvent.REQUEST_ALLOWED),
                        tuple(Level.INFO, RateLimitEvent.REQUEST_BLOCKED),
                        tuple(Level.INFO, RateLimitEvent.RULE_UPDATED),
                        tuple(Level.INFO, RateLimitEvent.RULE_DELETED));
    }

    @Test
    @DisplayName("At INFO the served requests are silent and the refusal still shows, with its key")
    void servedRequestsAreSilentAtInfo() throws Exception {
        logger.setLevel(Level.INFO);

        consumer.consumeMessage(List.of(
                message(RateLimitEvent.requestAllowed("abc-123", 7, 71, 100, 60)),
                message(RateLimitEvent.requestAllowed("abc-123", 7, 72, 100, 60)),
                message(RateLimitEvent.requestBlocked("abc-123", 7, 100, 100, 60))), null);

        // One line out of three: this is what keeps a production log readable once the
        // allowed events arrive at the rate of the busiest endpoint in the system.
        assertThat(appender.list).singleElement()
                .satisfies(event -> {
                    assertThat(event.getLevel()).isEqualTo(Level.INFO);
                    assertThat(event.getFormattedMessage())
                            .contains(RateLimitEvent.REQUEST_BLOCKED)
                            // The key is what makes the line actionable on its own.
                            .contains("apiKey=abc-123")
                            .contains("usage=100");
                });
    }

    @Test
    @DisplayName("A published event deserialises back into the same record")
    void aPublishedEventRoundTrips() throws Exception {
        RateLimitEvent event = RateLimitEvent.requestBlocked("abc-123", 7, 100, 100, 60);

        assertThat(objectMapper.readValue(objectMapper.writeValueAsString(event), RateLimitEvent.class))
                .isEqualTo(event);
    }

    @Test
    @DisplayName("An unreadable body is acknowledged rather than redelivered")
    void acknowledgesUnreadableMessages() {
        assertThat(consumer.consumeMessage(List.of(message("not json at all")), null))
                .isEqualTo(ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
    }

    private MessageExt message(RateLimitEvent event) throws Exception {
        return message(objectMapper.writeValueAsString(event));
    }

    private static MessageExt message(String body) {
        MessageExt message = new MessageExt();
        message.setBody(body.getBytes(StandardCharsets.UTF_8));
        return message;
    }
}
