package com.example.demo.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.demo.config.KafkaTopicsProperties;
import com.example.demo.dto.TransactionRequest;
import com.example.demo.event.TransactionEvent;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;

class TransactionEventProducerTest {

    @SuppressWarnings("unchecked")
    @Test
    void shouldPublishTransactionEventInTransaction() {
        KafkaTemplate<String, TransactionEvent> kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        KafkaTopicsProperties topics = new KafkaTopicsProperties("transaction-events", "transaction-received-events");
        TransactionEventProducer producer = new TransactionEventProducer(kafkaTemplate, topics);

        when(kafkaTemplate.executeInTransaction(any())).thenAnswer(invocation -> {
            KafkaOperations.OperationsCallback<String, TransactionEvent, Object> callback = invocation.getArgument(0);
            return callback.doInOperations(kafkaTemplate);
        });

        TransactionRequest request = new TransactionRequest(
                "txn-1",
                "user-1",
                120.4,
                "USD",
                Instant.parse("2026-03-01T18:30:00Z")
        );

        producer.send(request);

        verify(kafkaTemplate).send(eq("transaction-events"), eq("txn-1"), any(TransactionEvent.class));
    }
}
