package com.bbaayezi.kafkareplay.service;

import com.bbaayezi.kafkareplay.kafka.KafkaPartitionConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Service class responsible for replaying messages from Kafka topics.
 *
 * Provides methods to send messages to Kafka topics and consume messages
 * based on a specified timestamp range. The class uses KafkaTemplate for
 * producing messages and DefaultKafkaConsumerFactory for creating Kafka
 * consumers to consume messages from specific partitions of a topic.
 *
 * Key functions:
 * - Sending messages to specified Kafka topics.
 * - Consuming messages from a topic based on a timestamp range,
 *   across all its partitions.
 */
@Service
@Slf4j
public class KafkaReplayService {

    private static final String TEST_TOPIC = "test-topic";
    private static final String HELPER_CONSUMER_GROUP_PREFIX = "helper-TP-consumer";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory;

    @Autowired
    public KafkaReplayService(KafkaTemplate<String, String> kafkaTemplate, DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaConsumerFactory = kafkaConsumerFactory;
    }

    public void send(String payload) {
        kafkaTemplate.send(TEST_TOPIC, payload);
    }

    public void send(String topic, String payload) {
        kafkaTemplate.send(topic, payload);
    }

    public List<String> consumeMessages(String topic, long startingTimestamp, long endingTimestamp) {
        List<TopicPartition> partitions = getTopicPartitions(topic);
        if (partitions.isEmpty()) {
            throw new IllegalArgumentException("No partitions found for topic: " + topic);
        }
        log.info("Found {} partitions for topic: {}", partitions.size(), topic);

        List<String> aggregatedResults = Collections.synchronizedList(new ArrayList<>());

        ExecutorService executor = Executors.newFixedThreadPool(partitions.size());
        try {
            List<Future<Void>> futures = partitions.stream()
                    .map(partition -> executor.submit(() -> processPartition(partition, startingTimestamp, endingTimestamp, aggregatedResults)))
                    .toList();

            for (Future<Void> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Error while consuming messages: {}", e.getMessage());
                    Thread.currentThread().interrupt(); // preserve interrupt status
                }
            }
        } finally {
            executor.shutdown();
        }

        return aggregatedResults;
    }

    private Void processPartition(TopicPartition partition, long startingTimestamp, long endingTimestamp, List<String> aggregatedResults) {
        log.info("Processing partition: {}", partition);

        Consumer<String, String> consumer = kafkaConsumerFactory.createConsumer("-TP-" + partition.partition());
        try {
            OffsetAndTimestamp startingOffsetData = getOffsetForTimestamp(consumer, partition, startingTimestamp);
            if (startingOffsetData == null) {
                log.info("No messages found for partition: {} with the given timestamp range", partition);
                return null;
            }

            long startingOffset = startingOffsetData.offset();
            long endingOffset = calculateEndingOffset(consumer, partition, endingTimestamp);

            log.info("Partition: {} | Starting offset: {} | Ending offset: {}", partition.partition(), startingOffset, endingOffset);

            KafkaPartitionConsumer partitionConsumer = new KafkaPartitionConsumer(consumer);
            aggregatedResults.addAll(partitionConsumer.consumeFromRange(partition, startingOffset, endingOffset));
        } finally {
            consumer.close();
        }

        return null;
    }

    private long calculateEndingOffset(Consumer<String, String> consumer, TopicPartition partition, long endingTimestamp) {
        OffsetAndTimestamp endingOffsetData = getOffsetForTimestamp(consumer, partition, endingTimestamp);
        if (endingOffsetData == null) {
            long endingOffset = consumer.endOffsets(List.of(partition)).get(partition);
            log.info("No endingOffset based on timestamp for partition {}. Using partition end offset: {}", partition.partition(), endingOffset);
            return endingOffset;
        }
        return endingOffsetData.offset();
    }

    /**
     * Retrieves all partitions for the given Kafka topic and maps them into a list of TopicPartition objects.
     *
     * @param topic the name of the Kafka topic for which partitions information is to be retrieved
     * @return a list of TopicPartition objects representing the partitions of the specified topic
     */
    private List<TopicPartition> getTopicPartitions(String topic) {
        try (Consumer<String, String> partitionMetadataConsumer = kafkaConsumerFactory.createConsumer(HELPER_CONSUMER_GROUP_PREFIX)) {
            return partitionMetadataConsumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .collect(Collectors.toList());
        }
    }

    private OffsetAndTimestamp getOffsetForTimestamp(Consumer<String, String> consumer, TopicPartition partition, long timestamp) {
        Map<TopicPartition, Long> timestampMap = Collections.singletonMap(partition, timestamp);
        return consumer.offsetsForTimes(timestampMap).get(partition);
    }
}