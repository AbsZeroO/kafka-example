package org.example.notificationservice;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AnalyticsListener {

    private final OrderMapper orderMapper;

    public AnalyticsListener(OrderMapper orderMapper) {
        this.orderMapper = orderMapper;
    }

//    Kakfa lisiner
}