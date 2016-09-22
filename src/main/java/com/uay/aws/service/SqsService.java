package com.uay.aws.service;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqsService {

    private static final String QUEUE_URL = "QUEUE_URL";
    private static final int RECEIVE_ATTEMPTS = 10;
    private AmazonSQS sqs = new AmazonSQSClient().withRegion(Regions.EU_CENTRAL_1);
    private Map<String, String> receivedMessages;

    public Map<String, String> getMessages(LambdaLogger logger) {
        logger.log("Polling SQS");
        receivedMessages = new HashMap<>();

        for (int i = 0; i < RECEIVE_ATTEMPTS; i++) {
            ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(new ReceiveMessageRequest(QUEUE_URL));
            List<Message> messages = receiveMessageResult.getMessages();
            logger.log("Received messages: " + messages.size());
            messages.forEach(m -> processMessage(logger, m));
        }
        logger.log("Received " + receivedMessages.size() + " messages");
        return receivedMessages;
    }

    private void processMessage(LambdaLogger logger, Message message) {
        logger.log(message.getBody());
        receivedMessages.put(message.getReceiptHandle(), message.getBody());
    }

    public void deleteMessages(LambdaLogger logger, Map<String, String> messages) {
        if (messages.isEmpty()) return;
        logger.log("Deleting " + messages.size() + " messages");
        for (String receiptHandle : messages.keySet()) {
            sqs.deleteMessage(new DeleteMessageRequest(QUEUE_URL, receiptHandle));
        }
    }

}
