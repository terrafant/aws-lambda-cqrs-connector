package com.uay.aws.service;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.util.Map;

public class DynamoDbService {

    private static final String AGGREGATED_CQRS = "aggregated-cqrs";
    public static final AmazonDynamoDBClient CLIENT = new AmazonDynamoDBClient().withRegion(Regions.EU_CENTRAL_1);
//            .withEndpoint("http://localhost:8000");
    public static final String KEY_ID = "id";
    public static final String KEY_TEXT = "text";
    public static final String KEY_COUNT = "count";
    private DynamoDB dynamoDB = new DynamoDB(CLIENT);
    private Table table = dynamoDB.getTable(AGGREGATED_CQRS);

    public void postMessages(LambdaLogger logger, Map<String, String> messages) {
        if (messages.isEmpty()) return;
        logger.log("Sending to DynamoDB " + messages.size() + " messages");
        for (String message : messages.values()) {
            Item item = table.getItem(KEY_ID, message + "id");
            if (item == null) {
                item = new Item().withPrimaryKey(KEY_ID, message + "id").withString(KEY_TEXT, message).withNumber(KEY_COUNT, 0L);
            }
            item.withNumber(KEY_COUNT, item.getNumber(KEY_COUNT).longValue() + 1);
            table.putItem(item);
        }
    }

//    private void retrieveItem() {
//        ItemCollection<ScanOutcome> scan = table.scan(new ScanSpec());
//        ScanResult scanResult = CLIENT.scan(new ScanRequest().withTableName(AGGREGATED_CQRS));
//        for (Map<String, AttributeValue> item : scanResult.getItems()) {
//            System.out.println("item = " + item);
//        }
//    }
//
//    private void deleteExampleTable() {
//        Table table = dynamoDB.getTable(AGGREGATED_CQRS);
//        try {
//            System.out.println("Issuing DeleteTable request for " + AGGREGATED_CQRS);
//            table.delete();
//
//            System.out.println("Waiting for " + AGGREGATED_CQRS
//                    + " to be deleted...this may take a while...");
//
//            table.waitForDelete();
//        } catch (Exception e) {
//            System.err.println("DeleteTable request failed for " + AGGREGATED_CQRS);
//            System.err.println(e.getMessage());
//        }
//    }
//
//    public void createTable() throws InterruptedException {
//
//        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
//        attributeDefinitions.add(new AttributeDefinition()
//                .withAttributeName(KEY_ID)
//                .withAttributeType(ScalarAttributeType.S)
//        );
//
//        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
//        keySchema.add(new KeySchemaElement()
//                .withAttributeName(KEY_ID)
//                .withKeyType(KeyType.HASH)); //Partition key
//
//        CreateTableRequest request = new CreateTableRequest()
//                .withTableName(AGGREGATED_CQRS)
//                .withKeySchema(keySchema)
//                .withAttributeDefinitions(attributeDefinitions)
//                .withProvisionedThroughput(new ProvisionedThroughput()
//                        .withReadCapacityUnits(1L)
//                        .withWriteCapacityUnits(1L));
//
//        System.out.println("Issuing CreateTable request for " + AGGREGATED_CQRS);
//        table = dynamoDB.createTable(request);
//
//        System.out.println("Waiting for " + AGGREGATED_CQRS
//                + " to be created...this may take a while...");
//        table.waitForActive();
//    }
//
//
//    public static void main(String[] args) throws InterruptedException {
//        DynamoDbService dynamoDbService = new DynamoDbService();
////        dynamoDbService.deleteExampleTable();
////        dynamoDbService.createTable();
//        Map<String, String> messages = new HashMap<>();
//        messages.put("ddeiif94", "test1");
//        messages.put("apwwiif94", "test2");
//        messages.put("olkedif94", "test3");
//        dynamoDbService.postMessages(new LambdaLogger() {
//            @Override
//            public void log(String string) {
//                System.out.println(string);
//            }
//        }, messages);
//        dynamoDbService.retrieveItem();
//    }

}
