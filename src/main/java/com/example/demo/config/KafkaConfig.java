package com.example.demo.config;

import com.example.demo.event.TransactionEvent;
import com.example.demo.event.TransactionReceivedEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

@Configuration
public class KafkaConfig {

    @Bean
    public ProducerFactory<String, TransactionEvent> transactionEventProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl
    ) {
        Map<String, Object> props = producerProperties(bootstrapServers, schemaRegistryUrl);
        DefaultKafkaProducerFactory<String, TransactionEvent> factory = new DefaultKafkaProducerFactory<>(props);
        factory.setTransactionIdPrefix("tx-demo-");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, TransactionEvent> transactionEventKafkaTemplate(
            ProducerFactory<String, TransactionEvent> transactionEventProducerFactory
    ) {
        return new KafkaTemplate<>(transactionEventProducerFactory);
    }

    @Bean
    public KafkaTransactionManager<String, TransactionEvent> kafkaTransactionManager(
            ProducerFactory<String, TransactionEvent> transactionEventProducerFactory
    ) {
        return new KafkaTransactionManager<>(transactionEventProducerFactory);
    }

    @Bean
    public ProducerFactory<String, TransactionReceivedEvent> transactionReceivedProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl
    ) {
        return new DefaultKafkaProducerFactory<>(producerProperties(bootstrapServers, schemaRegistryUrl));
    }

    @Bean
    public KafkaTemplate<String, TransactionReceivedEvent> transactionReceivedKafkaTemplate(
            ProducerFactory<String, TransactionReceivedEvent> transactionReceivedProducerFactory
    ) {
        return new KafkaTemplate<>(transactionReceivedProducerFactory);
    }

    @Bean
    public ConsumerFactory<String, TransactionReceivedEvent> transactionReceivedConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl,
            @Value("${spring.kafka.consumer.group-id}") String groupId
    ) {
        return new DefaultKafkaConsumerFactory<>(consumerProperties(bootstrapServers, schemaRegistryUrl, groupId));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TransactionReceivedEvent> transactionReceivedKafkaListenerContainerFactory(
            ConsumerFactory<String, TransactionReceivedEvent> transactionReceivedConsumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, TransactionReceivedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(transactionReceivedConsumerFactory);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, TransactionEvent> transactionEventConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl,
            @Value("${app.kafka.processor-group-id}") String groupId
    ) {
        return new DefaultKafkaConsumerFactory<>(consumerProperties(bootstrapServers, schemaRegistryUrl, groupId));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> transactionEventKafkaListenerContainerFactory(
            ConsumerFactory<String, TransactionEvent> transactionEventConsumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(transactionEventConsumerFactory);
        return factory;
    }

    @Bean
    public NewTopic transactionEventsTopic(KafkaTopicsProperties topics) {
        return TopicBuilder.name(topics.transactionEvents())
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .build();
    }

    @Bean
    public NewTopic transactionReceivedEventsTopic(KafkaTopicsProperties topics) {
        return TopicBuilder.name(topics.transactionReceivedEvents())
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .build();
    }

    private static Map<String, Object> producerProperties(String bootstrapServers, String schemaRegistryUrl) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        return props;
    }

    private static Map<String, Object> consumerProperties(
            String bootstrapServers,
            String schemaRegistryUrl,
            String groupId
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return props;
    }
}
