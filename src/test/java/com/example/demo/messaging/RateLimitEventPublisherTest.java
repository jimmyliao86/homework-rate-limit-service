package com.example.demo.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willThrow;

import java.nio.charset.StandardCharsets;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The publisher's two promises: what goes on the wire, and that nothing comes back.
 *
 * <p>The failure tests are the reason this class exists. A publisher that throws on a
 * broker problem would turn a correctly rate-limited request into a 500 -- and it would do
 * so only in production, only when MQ is already having a bad day. Both ways that can
 * happen are exercised here: the send call failing outright, and the asynchronous callback
 * reporting the failure later.
 */
@ExtendWith(MockitoExtension.class)
class RateLimitEventPublisherTest {

    private static final String TOPIC = "RATE_LIMIT_EVENTS";
    private static final RateLimitEvent EVENT =
            RateLimitEvent.requestBlocked("abc-123", 7, 100, 100, 60);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private DefaultMQProducer producer;

    @Captor
    private ArgumentCaptor<Message> message;

    @Captor
    private ArgumentCaptor<SendCallback> callback;

    private RateLimitEventPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new RateLimitEventPublisher(producer, objectMapper, TOPIC);
    }

    @Test
    @DisplayName("The event is sent asynchronously, tagged with its type and keyed by API key")
    void sendsTheEventAsynchronously() throws Exception {
        publisher.publish(EVENT);

        // send(Message, SendCallback) rather than send(Message): the synchronous overload
        // would put a broker round trip inside the request the event describes.
        then(producer).should().send(message.capture(), any(SendCallback.class));

        assertThat(message.getValue().getTopic()).isEqualTo(TOPIC);
        assertThat(message.getValue().getTags()).isEqualTo(RateLimitEvent.REQUEST_BLOCKED);
        assertThat(message.getValue().getKeys()).isEqualTo("abc-123");
    }

    @Test
    @DisplayName("The body is JSON with epoch millis and 'limit' as the field name")
    void serialisesTheEventAsPlainJson() throws Exception {
        publisher.publish(EVENT);
        then(producer).should().send(message.capture(), any(SendCallback.class));

        JsonNode body = objectMapper.readTree(new String(message.getValue().getBody(), StandardCharsets.UTF_8));

        assertThat(body.get("eventType").asText()).isEqualTo("REQUEST_BLOCKED");
        assertThat(body.get("apiKey").asText()).isEqualTo("abc-123");
        assertThat(body.get("version").asLong()).isEqualTo(7);
        assertThat(body.get("usage").asLong()).isEqualTo(100);
        assertThat(body.get("windowSeconds").asInt()).isEqualTo(60);
        assertThat(body.get("eventId").asText()).isNotBlank();

        // The Java field is limitCount, because 'limit' is a MySQL reserved word; the wire
        // name follows the API, not the schema.
        assertThat(body.has("limitCount")).isFalse();
        assertThat(body.get("limit").asInt()).isEqualTo(100);

        // A number, not a string: an Instant here would need JavaTimeModule, and this test
        // uses a bare ObjectMapper precisely to prove none is required.
        assertThat(body.get("occurredAtEpochMs").isNumber()).isTrue();
        assertThat(body.get("occurredAtEpochMs").asLong()).isPositive();
    }

    @Test
    @DisplayName("A send that fails outright leaves the caller unaffected")
    void swallowsSendFailures() throws Exception {
        willThrow(new MQClientException("broker unreachable", null))
                .given(producer).send(any(Message.class), any(SendCallback.class));

        assertThatCode(() -> publisher.publish(EVENT)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("A failure reported through the callback leaves the caller unaffected")
    void swallowsAsynchronousFailures() throws Exception {
        publisher.publish(EVENT);
        then(producer).should().send(any(Message.class), callback.capture());

        assertThatCode(() -> callback.getValue().onException(new RuntimeException("send timed out")))
                .doesNotThrowAnyException();
    }
}
