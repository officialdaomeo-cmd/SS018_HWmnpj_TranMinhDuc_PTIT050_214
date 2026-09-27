package com.shopmart.order.service;

import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.dto.StockRequest;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;

@FeignClient(name = "inventory-service")
public interface InventoryClient {

    @GetMapping("/api/inventory/products/{id}")
    @CircuitBreaker(name = "inventory-service", fallbackMethod = "getProductByIdFallback")
    ProductDto getProductById(@PathVariable("id") Long id);

    @PutMapping("/api/inventory/products/{id}/decrease")
    @CircuitBreaker(name = "inventory-service", fallbackMethod = "decreaseStockFallback")
    ProductDto decreaseStock(@PathVariable("id") Long id, @RequestBody StockRequest request);

    default ProductDto getProductByIdFallback(Long id, Throwable t) {
        throw new RuntimeException("Inventory Service is unavailable for getProductById. Fallback triggered.");
    }

    default ProductDto decreaseStockFallback(Long id, StockRequest request, Throwable t) {
        throw new RuntimeException("Inventory Service is unavailable for decreaseStock. Fallback triggered.");
    }
}
