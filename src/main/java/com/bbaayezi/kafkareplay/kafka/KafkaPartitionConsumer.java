package com.bbaayezi.kafkareplay.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class KafkaPartitionConsumer {

    private final Consumer<String, String> kafkaConsumer;

    public KafkaPartitionConsumer(Consumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    /**
     * Consumes messages from a specific TopicPartition within a specified offset range.
     *
     * @param partition     the TopicPartition (topic + partition ID)
     * @param startingOffset the offset to start consuming from
     * @param endingOffset   the offset to stop consuming at (exclusive)
     * @return a List of messages consumed within the given offset range
     */
    public List<String> consumeFromRange(TopicPartition partition, long startingOffset, long endingOffset) {
        List<String> results = new ArrayList<>();

        try {
            // Assign the partition to the consumer
            kafkaConsumer.assign(List.of(partition));

            // Seek to the starting offset
            kafkaConsumer.seek(partition, startingOffset);

            // Poll messages and consume within the offset range
            boolean continuePolling = true;
            long startTime = System.currentTimeMillis();
            while (continuePolling && (System.currentTimeMillis() - startTime) < Duration.ofSeconds(5).toMillis()) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records.records(partition)) {
                    if (record.offset() >= startingOffset && record.offset() < endingOffset) {
                        // Add the record's value to the result
                        results.add(record.value());
                    }

                    // Stop polling if we reach or exceed the ending offset
                    if (record.offset() >= endingOffset - 1) {
                        continuePolling = false;
                        break;
                    }
                }
            }
        } finally {
            kafkaConsumer.close(); // Ensure the consumer is always closed to release resources
        }

        return results;
    }
}