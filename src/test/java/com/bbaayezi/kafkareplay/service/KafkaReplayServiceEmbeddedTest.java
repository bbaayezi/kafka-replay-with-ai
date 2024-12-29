package com.bbaayezi.kafkareplay.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {"multi-partition-topic"}, brokerProperties = {"log.dirs=target/kafka-logs"})
public class KafkaReplayServiceEmbeddedTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory;

    @Autowired
    private KafkaReplayService kafkaReplayService;

    private Consumer<String, String> testConsumer;

    @BeforeEach
    void setup() {
        testConsumer = kafkaConsumerFactory.createConsumer("test-group", "test-suffix");
        testConsumer.subscribe(Collections.singletonList("multi-partition-topic"));
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    void testConsumeMessagesWithMultiplePartitions() throws Exception {
        String topic = "multi-partition-topic";
        long startingTimestamp = System.currentTimeMillis();
        Thread.sleep(500); // Ensuring there's a delay

        // Producing messages to multiple partitions
        kafkaTemplate.send(new ProducerRecord<>(topic, 0, null, "partition-0-message1"));
        kafkaTemplate.send(new ProducerRecord<>(topic, 1, null, "partition-1-message1"));
        kafkaTemplate.send(new ProducerRecord<>(topic, 2, null, "partition-2-message1"));
        Thread.sleep(500); // Ensuring these get distinct timestamps
        long endingTimestamp = System.currentTimeMillis();

        // Producing more messages to ensure ending offset is calculated correctly
        kafkaTemplate.send(new ProducerRecord<>(topic, 0, null, "partition-0-message2"));
        kafkaTemplate.send(new ProducerRecord<>(topic, 1, null, "partition-1-message2"));
        kafkaTemplate.send(new ProducerRecord<>(topic, 2, null, "partition-2-message2"));

        // Wait for Kafka to process messages
        TimeUnit.SECONDS.sleep(2);

        // Consume messages using the KafkaReplayService
        List<String> messages = kafkaReplayService.consumeMessages(topic, startingTimestamp, endingTimestamp);

        // Assertions
        assertThat(messages).isNotNull();
        assertThat(messages).containsExactlyInAnyOrder(
                "partition-0-message1",
                "partition-1-message1",
                "partition-2-message1");

        assertThat(messages).doesNotContain(
                "partition-0-message2",
                "partition-1-message2",
                "partition-2-message2"); // These are outside the timestamp range
    }

    @Test
    void testConsumeMessagesFromEmbeddedKafkaSinglePartition() throws Exception {
        String topic = "single-partition-topic";
        long startingTimestamp = System.currentTimeMillis();
        long endingTimestamp = startingTimestamp + 1000;

        // Produce test messages
        kafkaTemplate.send(topic, "message1");
        Thread.sleep(500); // Ensuring timestamp difference
        kafkaTemplate.send(topic, "message2");

        // Wait for messages to be consumed
        Thread.sleep(2000);

        // Consume messages using the service
        List<String> messages = kafkaReplayService.consumeMessages(topic, startingTimestamp, endingTimestamp);

        assertThat(messages).isNotNull();
        assertThat(messages.size()).isEqualTo(2);
        assertThat(messages).contains("message1", "message2");
    }

    @Test
    void testConsumeMessagesWithEmptyTopic() {
        String topic = "empty-topic";
        long startingTimestamp = System.currentTimeMillis();
        long endingTimestamp = startingTimestamp + 1000;

        // Consume messages using the service
        List<String> messages = kafkaReplayService.consumeMessages(topic, startingTimestamp, endingTimestamp);

        assertThat(messages).isNotNull();
        assertThat(messages).isEmpty();
    }
}