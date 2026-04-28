package com.example.kafkabasic;


import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;
import java.util.List;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducer orderProducer;
    private final OrderConsumer orderConsumer;

    public OrderController(OrderProducer orderProducer, OrderConsumer orderConsumer) {
        this.orderProducer = orderProducer;
        this.orderConsumer = orderConsumer;
    }

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody OrderEvent request) {
        String orderId = UUID.randomUUID().toString();

        OrderEvent event = new OrderEvent(
                "OrderCreated",
                orderId,
                request.getProductId(),
                request.getQuantity(),
                request.getAmount()
        );

        orderProducer.sendOrderCreated(event);

        return ResponseEntity.ok("Order sent to Kafka. orderId=" + orderId);
    }

    @GetMapping("/producer-events")
    public ResponseEntity<List<OrderEvent>> getProducerEvents() {
        return ResponseEntity.ok(orderProducer.getSentEvents());
    }

    @GetMapping("/consumer-events")
    public ResponseEntity<List<OrderEvent>> getConsumerEvents() {
        return ResponseEntity.ok(orderConsumer.getConsumedEvents());
    }
}