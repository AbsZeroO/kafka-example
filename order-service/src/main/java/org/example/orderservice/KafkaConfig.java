package org.example.orderservice;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

@Configuration
public class KafkaConfig {

    @Bean
    public KafkaTransactionManager<String, OrderDto> kafkaTransactionManager(
            ProducerFactory<String, OrderDto> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }
}