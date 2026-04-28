package com.example.kafkabasic;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.util.LinkedList;
import java.util.List;
import java.util.Collections;

@Service
public class OrderProducer {

    private static final String TOPIC = "order-events";
    private static final int MAX_EVENTS = 100;

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final List<OrderEvent> sentEvents = Collections.synchronizedList(new LinkedList<>());

    public OrderProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderCreated(OrderEvent event) {
        kafkaTemplate.send(TOPIC, event.getOrderId(), event);
        sentEvents.add(event);
        if (sentEvents.size() > MAX_EVENTS) {
            sentEvents.remove(0);
        }
        System.out.println("[PRODUCER] sent: " + event);
    }

    public List<OrderEvent> getSentEvents() {
        return new LinkedList<>(sentEvents);
    }
}