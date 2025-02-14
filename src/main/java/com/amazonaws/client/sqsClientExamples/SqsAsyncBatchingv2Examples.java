package com.amazonaws.client.sqsClientExamples;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.batchmanager.SqsAsyncBatchManager;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

public class SqsAsyncBatchingv2Examples {
    public static void main(String[] args) {
        // Default Configuration via SqsAsyncClient.
        // This is the simplest approach, requiring minimal setup:
        SqsAsyncClient sqs = SqsAsyncClient.builder()
                .region(Region.US_EAST_1)
                .build();
        SqsAsyncBatchManager sqsAsyncBatchManager = sqs.batchManager();

        // Custom Configuration via SqsAsyncBatchManager.Builder:
        SqsAsyncBatchManager customizedBatchManager = SqsAsyncBatchManager.builder()
                .client(sqs)
                .scheduledExecutor(Executors.newScheduledThreadPool(5))
                .overrideConfiguration(b -> b
                        .maxBatchSize(10)
                        .sendRequestFrequency(Duration.ofMillis(200))
                        .receiveMessageMinWaitDuration(Duration.ofSeconds(10))
                        .receiveMessageVisibilityTimeout(Duration.ofSeconds(20))
                        .receiveMessageAttributeNames(Collections.singletonList("*"))
                        .receiveMessageSystemAttributeNames(Collections.singletonList(MessageSystemAttributeName.ALL)))
                .build();


        // create your queue
        final String queueName = "MyAsyncBufferedQueue" + UUID.randomUUID();
        final CreateQueueRequest request = CreateQueueRequest.builder().queueName(queueName).build();
        final String queueUrl = sqs.createQueue(request).join().queueUrl();
        System.out.println("Queue created: " + queueUrl);


        // Send messages
        CompletableFuture<SendMessageResponse> sendMessageFuture;
        for (int i = 0; i < 10; i++) {
            final int index = i;
            sendMessageFuture = sqsAsyncBatchManager.sendMessage(
                    r -> r.messageBody("Message " + index).queueUrl(queueUrl));
            SendMessageResponse response= sendMessageFuture.join();
            System.out.println("Message " + response.messageId() + " sent!");
        }

        // Receive messages with customized configurations
        CompletableFuture<ReceiveMessageResponse> receiveResponseFuture = customizedBatchManager.receiveMessage(
                r -> r.queueUrl(queueUrl)
                        .waitTimeSeconds(10)
                        .visibilityTimeout(20)
                        .maxNumberOfMessages(10)
        );
        System.out.println("You have received " + receiveResponseFuture.join().messages().size() + " messages in total.");

        // Delete messages
        DeleteQueueRequest deleteQueueRequest =  DeleteQueueRequest.builder().queueUrl(queueUrl).build();
        int code = sqs.deleteQueue(deleteQueueRequest).join().sdkHttpResponse().statusCode();
        System.out.println("Queue is deleted, with statusCode " + code);
    }
}
