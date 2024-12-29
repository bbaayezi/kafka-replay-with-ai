package com.bbaayezi.kafkareplay;

public record ConsumeRequest(
        String topic,
        String startingTimestamp,
        String endingTimestamp) { }
