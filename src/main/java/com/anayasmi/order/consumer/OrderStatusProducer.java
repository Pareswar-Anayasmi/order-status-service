package com.anayasmi.order.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderStatusProducer {

    private static final String ORDER_UPDATE_TOPIC = "order-status-update-topic";
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message) {
        System.out.println("Producing message: " + message);
        log.info("✅order update request sent to posgress api from kafka.");
        kafkaTemplate.send(ORDER_UPDATE_TOPIC, message);
    }
}
