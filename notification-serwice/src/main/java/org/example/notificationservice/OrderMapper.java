package org.example.notificationservice;

import org.springframework.stereotype.Component;

@Component
public class OrderMapper {

    public OrderEvent toDomain(OrderDto dto) {
        OrderEvent event = new OrderEvent();
        event.setOrderId(dto.orderId());
        event.setProductName(dto.productName());
        event.setPrice(dto.price());
        event.setSchemaVersion(1);
        return event;
    }
}