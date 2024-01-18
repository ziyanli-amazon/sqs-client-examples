package com.amazonaws.client.extendedClient;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit test for simple App.
 */
public class SqsExtendedClientExamplesTest {
    public static void main(String[] args) {
        final Map<String, MessageAttributeValue> msgValues = new HashMap<>();
        // msgValues.put("__type", new MessageAttributeValue().withStringValue("Client_id").withDataType("String"));
        final AmazonSQS sqsClient = AmazonSQSClientBuilder.standard().withRegion("us-east-1").build();
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.Log4JLogger");


        final CreateQueueRequest createQueueRequest = new CreateQueueRequest("sqsStandardQueueTest2");

        final String myQueueUrl = sqsClient.createQueue(createQueueRequest).getQueueUrl();

        final SendMessageRequest request = new SendMessageRequest()
                .withQueueUrl(myQueueUrl)
                .withMessageBody("This is a test for sqs standard queue")
                .withMessageAttributes(msgValues);

       SendMessageResult result = sqsClient.sendMessage(request);
       System.out.print(result.getMD5OfMessageAttributes());
       System.out.println(msgValues);

       ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest(myQueueUrl);
       ReceiveMessageResult response = sqsClient.receiveMessage(receiveRequest);
       if (response == null) {
           System.out.println("The response is Null");
       } else {
           System.out.println("It is empty: " + response);
       }

    }
}
