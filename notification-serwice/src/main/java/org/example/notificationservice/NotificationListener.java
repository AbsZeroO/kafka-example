package org.example.notificationservice;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class NotificationListener {
    @KafkaListener(topics = "orders-topic", groupId = "notification-group")
    public void consumeOrder(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
        System.out.println("-------------------------------------------------");
        System.out.println("Odebrano wiadomość z Kafki!");
        System.out.println("Wysyłam email potwierdzający dla klienta...");
        System.out.println("Zamówiono produkt: " + record.value().getProductName());
        System.out.println("Kwota: " + record.value().getPrice() + " PLN");
        System.out.println("-------------------------------------------------");
        ack.acknowledge();
    }
}