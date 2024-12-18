package com.sp.payment.service.impl;

import com.sp.payment.service.PaymentService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
public class PaymentServiceImpl implements PaymentService {

    private KafkaTemplate<String,String> kafkaTemplate;
    
    // Listen for order events from the "order-topic"
    @KafkaListener(topics = "order-topic", groupId = "payment-service-group")
    public void listenOrderEvent(String orderEvent) {
        // Process the order event (Parse the message and process payment)
        System.out.println("Received Order Event for Payment: " + orderEvent);

        // Process payment (Example: Integrate with a payment gateway)
        boolean isPaymentSuccessful = processPayment(orderEvent);

        // Send response to Order service after payment processing
        sendPaymentStatus(orderEvent, isPaymentSuccessful);
    }

    private void sendPaymentStatus(String orderEvent, boolean isPaymentSuccessful) {
        String paymentStatus = isPaymentSuccessful ? "Payment Successful" : "Payment Failed";

        // Send payment status event to Kafka for the Order Service to update the order
        String paymentResponseEvent = "OrderEvent: " + orderEvent + " | PaymentStatus: " + paymentStatus;
        kafkaTemplate.send("payment-status-topic", paymentResponseEvent);

        System.out.println("Payment status sent to Kafka: " + paymentResponseEvent);

    }

    private boolean processPayment(String orderEvent) {
        System.out.println("Processing payment for order event: " + orderEvent);

        Random random = new Random();
        boolean isPaymentSuccessful = random.nextInt(100) < 90;

        if (isPaymentSuccessful) {
            return true;  // Payment was successful
        } else {
            return false;  // Simulate payment failure
        }
    }
}
