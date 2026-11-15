package org.example.orderservice;

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

    public OrderDto toDto(OrderEvent event) {
        OrderDto dto = new OrderDto();
        dto.setOrderId(event.getOrderId());
        dto.setProductName(event.getProductName());
        dto.setPrice(event.getPrice());
        return dto;
    }
}