package com.example.demo.integration;

import com.example.demo.consumer.TransactionReceivedConsumer;
import com.example.demo.dto.TransactionRequest;
import com.example.demo.service.TransactionEventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
class TransactionFlowIT {

    private static final Network NETWORK = Network.newNetwork();

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.2"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka");

    @Container
    static final GenericContainer<?> SCHEMA_REGISTRY = new GenericContainer<>(
            DockerImageName.parse("confluentinc/cp-schema-registry:7.7.2"))
            .withNetwork(NETWORK)
            .dependsOn(KAFKA)
            .withExposedPorts(8081)
            .withEnv(Map.of(
                    "SCHEMA_REGISTRY_HOST_NAME", "schema-registry",
                    "SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081",
                    "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092"
            ));

    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", () ->
                "http://%s:%d".formatted(SCHEMA_REGISTRY.getHost(), SCHEMA_REGISTRY.getMappedPort(8081)));
    }

    @Autowired
    private TransactionEventProducer producer;

    @Autowired
    private TransactionReceivedConsumer transactionReceivedConsumer;

    @BeforeEach
    void setUp() {
        transactionReceivedConsumer.resetLatch();
    }

    @Test
    void shouldPublishTransactionEventAndReceiveTransactionReceivedEvent() {
        TransactionRequest request = new TransactionRequest(
                "txn-it-1",
                "user-it-1",
                55.12,
                "USD",
                Instant.now()
        );

        producer.send(request);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(transactionReceivedConsumer.awaitMessage(1, TimeUnit.SECONDS)).isTrue();
            assertThat(transactionReceivedConsumer.getLastReceivedEvent()).isNotNull();
            assertThat(transactionReceivedConsumer.getLastReceivedEvent().getTransactionId()).isEqualTo("txn-it-1");
            assertThat(transactionReceivedConsumer.getLastReceivedEvent().getStatus()).isEqualTo("RECEIVED");
        });
    }
}
