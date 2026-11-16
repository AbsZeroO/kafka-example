package org.example.notificationservice;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class NotificationListener {

    private final OrderMapper orderMapper;

    public NotificationListener(OrderMapper orderMapper) {
        this.orderMapper = orderMapper;
    }

//    Kafka Lisiner
}