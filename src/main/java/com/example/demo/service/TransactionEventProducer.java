package com.example.demo.service;

import com.example.demo.config.KafkaTopicsProperties;
import com.example.demo.dto.TransactionRequest;
import com.example.demo.event.TransactionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class TransactionEventProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionEventProducer.class);

    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;
    private final KafkaTopicsProperties topics;

    public TransactionEventProducer(
            KafkaTemplate<String, TransactionEvent> kafkaTemplate,
            KafkaTopicsProperties topics
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
    }

    public void send(TransactionRequest request) {
        TransactionEvent event = TransactionEvent.newBuilder()
                .setTransactionId(request.transactionId())
                .setUserId(request.userId())
                .setAmount(request.amount())
                .setCurrency(request.currency())
                .setTimestamp(resolveTimestamp(request))
                .build();

        kafkaTemplate.executeInTransaction(operations ->
                operations.send(topics.transactionEvents(), event.getTransactionId(), event)
        );

        log.info("Published TransactionEvent: transactionId={}, userId={}, amount={}, currency={}",
                event.getTransactionId(), event.getUserId(), event.getAmount(), event.getCurrency());
    }

    private static Instant resolveTimestamp(TransactionRequest request) {
        return request.timestamp() == null ? Instant.now() : request.timestamp();
    }
}
