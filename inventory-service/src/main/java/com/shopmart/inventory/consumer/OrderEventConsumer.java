package com.shopmart.inventory.consumer;

import com.shopmart.inventory.event.OrderEvent;
import com.shopmart.inventory.event.SagaEventType;
import com.shopmart.inventory.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final ProductService productService;

    @KafkaListener(topics = "order", groupId = "shopmart-inventory-group")
    public void consume(OrderEvent event) {
        log.info("Inventory service received event: {}", event);
        if (event.getType() == SagaEventType.PAYMENT_FAILED) {
            // Compensating transaction: hoàn lại tồn kho
            log.info("Payment failed for orderId={}, rolling back inventory for productId={}, quantity={}", 
                    event.getOrderId(), event.getProductId(), event.getQuantity());
            try {
                productService.increaseStock(event.getProductId(), event.getQuantity());
                log.info("Inventory rollback successful for orderId={}", event.getOrderId());
            } catch (Exception e) {
                log.error("Failed to rollback inventory for orderId={}", event.getOrderId(), e);
            }
        }
    }
}
