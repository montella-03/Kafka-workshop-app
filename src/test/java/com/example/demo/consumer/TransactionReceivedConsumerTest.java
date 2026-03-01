package com.example.demo.consumer;

import com.example.demo.config.KafkaTopicsProperties;
import com.example.demo.event.TransactionReceivedEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionReceivedConsumerTest {

    @Test
    void shouldStoreAndExposeLastReceivedEvent() throws Exception {
        TransactionReceivedConsumer consumer = new TransactionReceivedConsumer(
                new KafkaTopicsProperties("transaction-events", "transaction-received-events")
        );
        consumer.resetLatch();

        TransactionReceivedEvent event = TransactionReceivedEvent.newBuilder()
                .setTransactionId("txn-2")
                .setUserId("user-2")
                .setStatus("RECEIVED")
                .setMessage("ok")
                .setTimestamp(Instant.parse("2026-03-01T19:00:00Z"))
                .build();

        consumer.listen(event);

        assertThat(consumer.awaitMessage(1, TimeUnit.SECONDS)).isTrue();
        assertThat(consumer.getLastReceivedEvent()).isEqualTo(event);
        assertThat(consumer.getTopicName()).isEqualTo("transaction-received-events");
    }
}
