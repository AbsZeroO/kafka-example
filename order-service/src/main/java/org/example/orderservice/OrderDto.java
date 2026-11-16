package org.example.orderservice;

public record OrderDto(String orderId, String productName, Double price) {
}
