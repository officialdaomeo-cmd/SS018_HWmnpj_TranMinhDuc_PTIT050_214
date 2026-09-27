package com.shopmart.payment.consumer;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;
import com.shopmart.payment.entity.PaymentStatus;
import com.shopmart.payment.event.OrderEvent;
import com.shopmart.payment.event.SagaEventType;
import com.shopmart.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final PaymentService paymentService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "order", groupId = "shopmart-group")
    public void consume(OrderEvent event) {
        log.info("Payment service received event: {}", event);
        if (event.getType() == SagaEventType.ORDER_CREATED) {
            try {
                PaymentRequest request = new PaymentRequest();
                request.setOrderId(event.getOrderId());
                request.setAmount(event.getAmount());
                
                PaymentResponse response = paymentService.processPayment(request);
                
                if (response.getStatus() == PaymentStatus.SUCCESS) {
                    event.setType(SagaEventType.PAYMENT_COMPLETED);
                } else {
                    event.setType(SagaEventType.PAYMENT_FAILED);
                    event.setMessage(response.getMessage());
                }
            } catch (Exception e) {
                log.error("Payment failed with exception", e);
                event.setType(SagaEventType.PAYMENT_FAILED);
                event.setMessage(e.getMessage());
            }
            kafkaTemplate.send("order", event);
            log.info("Published {} event for order id={}", event.getType(), event.getOrderId());
        }
    }
}
