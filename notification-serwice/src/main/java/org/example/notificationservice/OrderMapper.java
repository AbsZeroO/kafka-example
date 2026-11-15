package org.example.notificationservice;

import org.springframework.stereotype.Component;

@Component
public class OrderMapper {

    public OrderEvent toDomain(OrderDto dto) {
        OrderEvent event = new OrderEvent();
        event.setOrderId(dto.getOrderId());
        event.setProductName(dto.getProductName());
        event.setPrice(dto.getPrice());
        event.setSchemaVersion(1);
        return event;
    }
}