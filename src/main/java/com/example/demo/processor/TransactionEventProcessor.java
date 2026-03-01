package com.example.demo.processor;

import com.example.demo.config.KafkaTopicsProperties;
import com.example.demo.event.TransactionEvent;
import com.example.demo.event.TransactionReceivedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class TransactionEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransactionEventProcessor.class);

    private final KafkaTemplate<String, TransactionReceivedEvent> transactionReceivedKafkaTemplate;
    private final KafkaTopicsProperties topics;

    public TransactionEventProcessor(
            KafkaTemplate<String, TransactionReceivedEvent> transactionReceivedKafkaTemplate,
            KafkaTopicsProperties topics
    ) {
        this.transactionReceivedKafkaTemplate = transactionReceivedKafkaTemplate;
        this.topics = topics;
    }

    @KafkaListener(
            topics = "#{@kafkaTopicsProperties.transactionEvents()}",
            groupId = "${app.kafka.processor-group-id}",
            containerFactory = "transactionEventKafkaListenerContainerFactory"
    )
    public void process(TransactionEvent event) {
        log.info("Processing TransactionEvent: transactionId={}, amount={}, currency={}",
                event.getTransactionId(), event.getAmount(), event.getCurrency());

        TransactionReceivedEvent receivedEvent = TransactionReceivedEvent.newBuilder()
                .setTransactionId(event.getTransactionId())
                .setUserId(event.getUserId())
                .setStatus("RECEIVED")
                .setMessage("Transaction accepted for downstream processing")
                .setTimestamp(Instant.now())
                .build();

        transactionReceivedKafkaTemplate.send(topics.transactionReceivedEvents(), receivedEvent.getTransactionId(), receivedEvent);
    }
}
