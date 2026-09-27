package com.shopmart.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

// TODO Câu 1: Đăng ký service vào Eureka
@SpringBootApplication
@org.springframework.cloud.client.discovery.EnableDiscoveryClient
@ConfigurationPropertiesScan
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
