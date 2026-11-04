package org.example.notificationservice;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AnalyticsListener {
    @KafkaListener(topics = "orders-topic", groupId = "analytics-group")
    public void consumeOrder(OrderEvent orderEvent) {
        System.out.println("[analytics] Zamówienie: " + orderEvent.getProductName() + " kwota=" + orderEvent.getPrice());
    }
}