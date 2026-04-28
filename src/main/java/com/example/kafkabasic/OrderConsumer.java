package com.example.kafkabasic;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.util.LinkedList;
import java.util.List;
import java.util.Collections;

@Service
public class OrderConsumer {

    private static final int MAX_EVENTS = 100;
    private final List<OrderEvent> consumedEvents = Collections.synchronizedList(new LinkedList<>());

    @KafkaListener(topics = "order-events", groupId = "order-demo-group")
    public void consume(OrderEvent event) {
        consumedEvents.add(event);
        if (consumedEvents.size() > MAX_EVENTS) {
            consumedEvents.remove(0);
        }
        System.out.println("[CONSUMER] " + event);
    }

    public List<OrderEvent> getConsumedEvents() {
        return new LinkedList<>(consumedEvents);
    }
}