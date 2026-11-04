package org.example.orderservice;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderController(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public ResponseEntity<?> placeOrder(@RequestBody OrderEvent orderEvent) {
        if (orderEvent.getOrderId() == null) {
            orderEvent.setOrderId(UUID.randomUUID().toString());
        }

        String correlationId = UUID.randomUUID().toString();
        ProducerRecord<String, OrderEvent> record = new ProducerRecord<>("orders-topic", orderEvent.getOrderId(), orderEvent);
        record.headers().add("X-Correlation-Id", correlationId.getBytes());

        System.out.println("Otrzymano żądanie HTTP. Przetwarzanie zamówienia: " + orderEvent.getProductName());
        kafkaTemplate.send(record);

        return ResponseEntity.ok().body("Zamówienie " + orderEvent.getOrderId() + " przekazane do Kafki!");
    }
}