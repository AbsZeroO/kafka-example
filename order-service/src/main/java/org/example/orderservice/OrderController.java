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
        if (orderDto.getOrderId() == null) {
            orderDto.setOrderId(UUID.randomUUID().toString());
        }

        OrderEvent orderEvent = orderMapper.toDomain(orderDto);

        String correlationId = UUID.randomUUID().toString();
        ProducerRecord<String, OrderDto> record =
                new ProducerRecord<>("orders-topic", orderDto.getOrderId(), orderDto);
        record.headers().add("X-Correlation-Id", correlationId.getBytes());

        System.out.println("Otrzymano żądanie HTTP. Przetwarzanie zamówienia: " + orderEvent.getProductName());
        kafkaTemplate.send(record);

        return ResponseEntity.ok()
                .body("Zamówienie " + orderDto.getOrderId() + " przekazane do Kafki!");
    }
}