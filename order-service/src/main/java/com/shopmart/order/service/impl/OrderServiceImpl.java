package com.shopmart.order.service.impl;

import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.entity.Order;
import com.shopmart.order.entity.OrderStatus;
import com.shopmart.order.exception.ResourceNotFoundException;
import com.shopmart.order.repository.OrderRepository;
import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final InventoryClient inventoryClient;
    private final org.springframework.kafka.core.KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    @Transactional
    public OrderResponse createOrder(OrderRequest request) {
        // TODO Câu 2: Gọi inventory-service qua FeignClient (có Circuit Breaker + fallback)
        //            để lấy thông tin sản phẩm -> tính totalAmount = price * quantity
        var product = inventoryClient.getProductById(request.getProductId());
        
        // Decrease stock synchronously or handle in Saga. The requirement says:
        // "Giao dịch đặt hàng gồm nhiều bước trên nhiều service (trừ tồn kho inventory-service, thanh toán payment-service, ghi đơn order-service)..."
        // Wait, normally we deduct stock via Feign synchronously or via event. The requirement says:
        // "Khi một bước fail thì kích hoạt compensating/rollback hoàn tác phần đã thực hiện (vd. hoàn tồn kho, hoàn tiền)."
        // It says "Order-service cần gọi inventory-service để kiểm tra/trừ tồn kho nhưng không phụ thuộc cứng. Hãy làm lớp gọi service theo tên và chống lỗi liên kết: Tạo FeignClient... lấy thông tin sản phẩm và trừ tồn kho."
        // So I should call decreaseStock via FeignClient here.
        var stockRequest = new com.shopmart.order.dto.StockRequest();
        stockRequest.setQuantity(request.getQuantity());
        inventoryClient.decreaseStock(request.getProductId(), stockRequest);

        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .status(OrderStatus.PENDING)
                .build();
        Order saved = orderRepository.save(order);
        log.info("Created order id={} with status PENDING", saved.getId());

        // TODO Câu 3: Publish OrderEvent (type = ORDER_CREATED) lên Kafka topic "order" để khởi động Saga
        com.shopmart.order.event.OrderEvent event = new com.shopmart.order.event.OrderEvent();
        event.setOrderId(saved.getId());
        event.setCustomerId(saved.getCustomerId());
        event.setProductId(saved.getProductId());
        event.setQuantity(saved.getQuantity());
        // Assume price * quantity
        event.setAmount(product.getPrice().multiply(java.math.BigDecimal.valueOf(saved.getQuantity())));
        event.setEventType("ORDER_CREATED");

        kafkaTemplate.send("order", event);
        log.info("Published ORDER_CREATED event for order id={}", saved.getId());

        return OrderResponse.from(saved);
    }

    @Override
    public OrderResponse getOrderById(Long id) {
        return OrderResponse.from(findOrder(id));
    }

    @Override
    public List<OrderResponse> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(OrderResponse::from)
                .toList();
    }

    @Override
    @Transactional
    public OrderResponse completeOrder(Long orderId) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.COMPLETED);
        order.setFailureReason(null);
        log.info("Order id={} COMPLETED", orderId);
        return OrderResponse.from(orderRepository.save(order));
    }

    @Override
    @Transactional
    public OrderResponse cancelOrder(Long orderId, String reason) {
        Order order = findOrder(orderId);
        order.setStatus(OrderStatus.CANCELLED);
        order.setFailureReason(reason);
        log.error("Order id={} CANCELLED: {}", orderId, reason);
        return OrderResponse.from(orderRepository.save(order));
    }

    private Order findOrder(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy đơn hàng id=" + id));
    }
}
