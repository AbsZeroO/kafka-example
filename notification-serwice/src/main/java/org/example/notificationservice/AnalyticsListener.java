package org.example.notificationservice;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AnalyticsListener {

    private final OrderMapper orderMapper;

    public AnalyticsListener(OrderMapper orderMapper) {
        this.orderMapper = orderMapper;
    }

    @KafkaListener(topics = "orders-topic", groupId = "analytics-group")
    public void consumeOrder(OrderDto orderDto) {
        OrderEvent orderEvent = orderMapper.toDomain(orderDto);
        System.out.println("[analytics] Zamówienie: " + orderEvent.getProductName() + " kwota=" + orderEvent.getPrice());
    }
}