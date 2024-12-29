package com.bbaayezi.kafkareplay.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@Profile("local-kafka-listener")
@Slf4j
public class LocalKafkaListener {
    @KafkaListener(topics = "multi-partition-topic", groupId = "test-group")
    public void listen(String message, 
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
//                       @Header(KafkaHeaders.RECEIVED_KEY) String key,
                       @Header(KafkaHeaders.OFFSET) long offset,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp) {
        log.info("Received message: {} | Topic: {} | Partition: {} | Offset: {} | Timestamp: {}",
                message, topic, partition, offset, timestamp);
    }
}
