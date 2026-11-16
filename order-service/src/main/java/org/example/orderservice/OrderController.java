package org.example.orderservice;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final KafkaTemplate<String, OrderDto> kafkaTemplate;
    private final OrderMapper orderMapper;
    private final KafkaTransactionManager<String, OrderDto> transactionManager;

    public OrderController(KafkaTemplate<String, OrderDto> kafkaTemplate,
                           OrderMapper orderMapper,
                           KafkaTransactionManager<String, OrderDto> transactionManager) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderMapper = orderMapper;
        this.transactionManager = transactionManager;
    }

    @PostMapping
    @Transactional
    public ResponseEntity<?> placeOrder(@RequestBody OrderDto orderDto) {

    }
}