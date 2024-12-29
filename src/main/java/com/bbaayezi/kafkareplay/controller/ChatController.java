package com.bbaayezi.kafkareplay.controller;

import com.bbaayezi.kafkareplay.ConsumeRequest;
import com.bbaayezi.kafkareplay.service.KafkaReplayService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

@RestController
@Slf4j
public class ChatController {

    private final ChatClient chatClient;
    private final KafkaReplayService kafkaReplayService;

    @Autowired
    public ChatController(ChatClient.Builder builder, KafkaReplayService kafkaReplayService) {
        this.chatClient = builder
                .defaultSystem("""
                You are a helpful AI assistant that will parse the user input to the application input.
                The application is a Kafka Replay tool which is expecting a topic name, starting timestamp and ending timestamp for input.
                The application input is a JSON format file with three String fields, with keys named "topic", "startingTimestamp" 
                and "endingTimestamp" respectively. You will be expecting to extract a time range from the users input 
                and convert them into fixed datetime string with format "yyyy-MM-dd hh:mm:ss.SSS" and pass to the "startingTimestamp" and "endingTimestamp". The user 
                will also provide you with the input of a topic name.
                Your reply will either be a successfully parsed JSON, or a String of error message if you failed to understand
                user's input, the error message will be the reason why you don't understand the user, for example user is
                not providing a topic name.
                Please make sure the output you generate always sticks to the requirement listed above. You don't need to generate any other contents or notes in your response.
                """)
                .build();
        this.kafkaReplayService = kafkaReplayService;
    }

    @GetMapping("/chat/consume")
    public ResponseEntity<List<String>> consumeMessages(@RequestBody ConsumeRequest consumeRequest) {
        List<String> consumedMessages = kafkaReplayService.consumeMessages(
                consumeRequest.topic(),
                Long.parseLong(consumeRequest.startingTimestamp()),
                Long.parseLong(consumeRequest.endingTimestamp())
        );
        return ResponseEntity.ok(consumedMessages);
    }

    @GetMapping("/kafka")
    public ResponseEntity<List<String>> consumeFromAIGenerated(@RequestBody String userInput) throws ParseException {

        var callResponse = chatClient.prompt()
                .user(userInput)
                .call();
        var parsedInput = callResponse.entity(ConsumeRequest.class);
        var dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        var parsedStart = dateFormat.parse(parsedInput.startingTimestamp()).getTime();
        var parsedEnd = dateFormat.parse(parsedInput.endingTimestamp()).getTime();
        log.info("AI response: {}. Parsed to APP input: {}", callResponse.chatResponse(), parsedInput);

        List<String> consumedMessages = kafkaReplayService.consumeMessages(
                parsedInput.topic(),
                parsedStart,
                parsedEnd
        );
        return ResponseEntity.ok(consumedMessages);
    }

}
