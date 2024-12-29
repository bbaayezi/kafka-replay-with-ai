package com.bbaayezi.kafkareplay.controller;

import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import org.springframework.beans.factory.annotation.Autowired;
import com.bbaayezi.kafkareplay.service.KafkaReplayService;

import java.util.List;

@RestController
public class KafkaReplayController {

    
    private final KafkaReplayService kafkaReplayService;

    @Autowired
    public KafkaReplayController(KafkaReplayService kafkaReplayService) {
        this.kafkaReplayService = kafkaReplayService;
    }

    @PostMapping("/produce")
    public ResponseEntity<String> logPayload(@RequestBody String payload) {
        kafkaReplayService.send("multi-partition-topic", payload);
        return ResponseEntity.ok("Message sent to Kafka topic");
    }
    
    
    
    @GetMapping("/consume")
    public ResponseEntity<List<String>> consumeMessages(
            @RequestParam("topic") String topic,
            @RequestParam("startingTimestamp") long startingTimestamp,
            @RequestParam("endingTimestamp") long endingTimestamp) {
        List<String> consumedMessages = kafkaReplayService.consumeMessages(topic, startingTimestamp, endingTimestamp);
        return ResponseEntity.ok(consumedMessages);
    }
}
