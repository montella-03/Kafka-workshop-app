package com.example.demo.consumer;

import com.example.demo.config.KafkaTopicsProperties;
import com.example.demo.event.TransactionReceivedEvent;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionReceivedConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionReceivedConsumer.class);

    private final KafkaTopicsProperties topics;
    private final AtomicReference<TransactionReceivedEvent> lastReceivedEvent = new AtomicReference<>();
    private final AtomicReference<CountDownLatch> latchRef = new AtomicReference<>(new CountDownLatch(1));

    public TransactionReceivedConsumer(KafkaTopicsProperties topics) {
        this.topics = topics;
    }

    @KafkaListener(
            topics = "#{@kafkaTopicsProperties.transactionReceivedEvents()}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "transactionReceivedKafkaListenerContainerFactory"
    )
    public void listen(TransactionReceivedEvent event) {
        lastReceivedEvent.set(event);
        latchRef.get().countDown();
        log.info("Received TransactionReceivedEvent: transactionId={}, status={}, message={}",
                event.getTransactionId(), event.getStatus(), event.getMessage());
    }

    public void resetLatch() {
        latchRef.set(new CountDownLatch(1));
    }

    public boolean awaitMessage(long timeout, TimeUnit unit) throws InterruptedException {
        return latchRef.get().await(timeout, unit);
    }

    public TransactionReceivedEvent getLastReceivedEvent() {
        return lastReceivedEvent.get();
    }

    public String getTopicName() {
        return topics.transactionReceivedEvents();
    }
}
