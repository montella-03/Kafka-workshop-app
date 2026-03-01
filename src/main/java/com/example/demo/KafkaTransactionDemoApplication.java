package com.example.demo;

import com.example.demo.config.KafkaTopicsProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(KafkaTopicsProperties.class)
public class KafkaTransactionDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaTransactionDemoApplication.class, args);
    }
}
