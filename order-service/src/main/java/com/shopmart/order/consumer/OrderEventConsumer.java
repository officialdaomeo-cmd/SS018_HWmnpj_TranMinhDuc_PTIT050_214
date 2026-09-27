package com.shopmart.order.consumer;

import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final OrderService orderService;

    @KafkaListener(topics = "order", groupId = "shopmart-order-group")
    public void consume(OrderEvent event) {
        log.info("Order service received event: {}", event);
        if (event.getType() == SagaEventType.PAYMENT_COMPLETED) {
            orderService.completeOrder(event.getOrderId());
            log.info("Order id={} marked as COMPLETED", event.getOrderId());
        } else if (event.getType() == SagaEventType.PAYMENT_FAILED) {
            orderService.cancelOrder(event.getOrderId(), event.getMessage());
            log.info("Order id={} marked as CANCELLED due to payment failure", event.getOrderId());
        }
    }
}
