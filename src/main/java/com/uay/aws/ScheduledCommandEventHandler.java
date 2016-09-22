package com.uay.aws;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.uay.aws.domain.ScheduledEvent;
import com.uay.aws.service.DynamoDbService;
import com.uay.aws.service.SqsService;

import java.util.Map;

public class ScheduledCommandEventHandler implements RequestHandler<ScheduledEvent, String> {

    private SqsService sqsService = new SqsService();
    private DynamoDbService dynamoDbService = new DynamoDbService();

    @Override
    public String handleRequest(ScheduledEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("event = " + event.toJson());

        Map<String, String> receivedMessages = sqsService.getMessages(logger);
        dynamoDbService.postMessages(logger, receivedMessages);
        sqsService.deleteMessages(logger, receivedMessages);

        return "Finished processing event";
    }
}

