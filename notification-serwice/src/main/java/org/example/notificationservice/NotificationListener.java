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

    @KafkaListener(topics = "orders-topic", groupId = "notification-group")
    public void consumeOrder(ConsumerRecord<String, OrderDto> record, Acknowledgment ack) {
        OrderDto dto = record.value();
        OrderEvent orderEvent = orderMapper.toDomain(dto);

        System.out.println("-------------------------------------------------");
        System.out.println("Odebrano wiadomość z Kafki!");
        System.out.println("Temat: " + record.topic());
        System.out.println("Partycja: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Klucz: " + record.key());
        System.out.println("Wysyłam email potwierdzający dla klienta...");
        System.out.println("Zamówiono produkt: " + orderEvent.getProductName());
        System.out.println("Kwota: " + orderEvent.getPrice() + " PLN");
        System.out.println("-------------------------------------------------");
        ack.acknowledge();
    }
}