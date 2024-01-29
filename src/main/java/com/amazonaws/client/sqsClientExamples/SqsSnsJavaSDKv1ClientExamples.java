package com.amazonaws.client.sqsClientExamples;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.client.sqsClientExamples.Constants.REGION;
import static com.amazonaws.client.sqsClientExamples.Constants.SNS_SUBSCRIPTION_PROTOCOL;
import static com.amazonaws.client.sqsClientExamples.Utils.plainTextGenerator;
import static com.amazonaws.client.sqsClientExamples.Utils.stringBytesCalculator;

/** Send and Receive messages from SNS to SQS using AWS Java SDK 1.x */
public class SqsSnsJavaSDKv1ClientExamples {

  static final String TOPIC_NAME = UUID.randomUUID() + "-sns-sqs-topic";
  static final String QUEUE_NAME = UUID.randomUUID() + "-sns-sqs-queue";
  static String queueArn;
  static String topicArn;
  static String messageBody;
  static String queueUrl;
  static String policy;

  public static void main(String[] args) throws UnsupportedEncodingException {
    // Create ana SNS and SQS Client
    final AmazonSNS snsClient = AmazonSNSClientBuilder.standard().withRegion(REGION).build();
    final AmazonSQS sqsClient = AmazonSQSClientBuilder.standard().withRegion(REGION).build();

    // Create an SNS Topic
    CreateTopicRequest createTopicRequest = new CreateTopicRequest(TOPIC_NAME);
    CreateTopicResult createTopicResult = snsClient.createTopic(createTopicRequest);
    topicArn = createTopicResult.getTopicArn();
    System.out.println("An SNS topic has been created and the TopicArn is: " + topicArn);

    // Create an SQS Queue
    CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_NAME);
    CreateQueueResult createQueueResult = sqsClient.createQueue(createQueueRequest);
    queueUrl = createQueueResult.getQueueUrl();
    System.out.println("An SQS queue has been created and the QueueUrl is: " + queueUrl);
    GetQueueAttributesRequest getQueueAttributesRequest =
        new GetQueueAttributesRequest(queueUrl).withAttributeNames("QueueArn");
    GetQueueAttributesResult getQueueAttributesResult =
        sqsClient.getQueueAttributes(getQueueAttributesRequest);
    queueArn = getQueueAttributesResult.getAttributes().get("QueueArn");

    // Set the queue attributes map with the policy
    Map<String, String> attributes = getAttributes(queueArn, topicArn);
    System.out.println(
        "Queue Policy has been set up correctly in order to send message from SNS topic.");

    // Set the queue attributes
    SetQueueAttributesRequest request = new SetQueueAttributesRequest(queueUrl, attributes);
    sqsClient.setQueueAttributes(request);

    // Subscribe the SQS queue to the SNS topic
    SubscribeRequest subscribeRequest =
        new SubscribeRequest(topicArn, SNS_SUBSCRIPTION_PROTOCOL, queueArn);
    snsClient.subscribe(subscribeRequest);

    // Publish the message to the SNS topic
    messageBody = plainTextGenerator(16, false);
    int messageBodySize = stringBytesCalculator(messageBody);
    System.out.println("Message Body is: " + messageBody);
    System.out.println("Message Body Size is: " + messageBodySize);
    PublishRequest publishRequest = new PublishRequest(topicArn, messageBody);
    PublishResult publishResult = snsClient.publish(publishRequest);
    if (publishResult.getSdkHttpMetadata().getHttpStatusCode() == 200) {
      System.out.println(
          "Message published successfully!" + "\n" + "MessageId: " + publishResult.getMessageId());
      System.out.println("Technically, a Url encoding process will happen");
    }

    // Receive the message from the SQS queue
    ReceiveMessageRequest receiveMessageRequest =
        new ReceiveMessageRequest().withQueueUrl(queueUrl).withWaitTimeSeconds(5);
    ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
    if (!receiveMessageResult.getMessages().isEmpty()) {
      String receivedMessageBody = receiveMessageResult.getMessages().get(0).getBody();
      System.out.println("Messages received from the queue.");
      System.out.println(
          "The Message Body Size is: "
              + stringBytesCalculator(Utils.encodeUrl(receivedMessageBody)));
      System.out.println("The Message Body is: " + receivedMessageBody);
      System.out.println("The Message Body Size is: " + stringBytesCalculator(receivedMessageBody));

      // Delete the Message
      String receiptHandle = receiveMessageResult.getMessages().get(0).getReceiptHandle();
      sqsClient.deleteMessage(queueUrl, receiptHandle);
      System.out.println("Message deleted successfully!");
    } else {
      System.out.println("No Messages received.");
    }

    deleteResources(snsClient, sqsClient);
    System.out.println("Resources destroyed successfully!");
  }

  private static Map<String, String> getAttributes(String queueArn, String topicArn) {
    Map<String, String> attributes = new HashMap<>();
    policy =
        "{\n"
            + "  \"Version\": \"2012-10-17\",\n"
            + "  \"Statement\": [\n"
            + "    {\n"
            + "      \"Effect\": \"Allow\",\n"
            + "      \"Principal\": \"*\",\n"
            + "      \"Action\": \"sqs:SendMessage\",\n"
            + "      \"Resource\": \""
            + queueArn
            + "\",\n"
            + "      \"Condition\": {\n"
            + "        \"ArnEquals\": { \"aws:SourceArn\": \""
            + topicArn
            + "\" }\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    attributes.put(QueueAttributeName.Policy.name(), policy);
    return attributes;
  }

  private static void deleteResources(AmazonSNS snsClient, AmazonSQS sqsClient) {
    // List subscriptions for the specified topic
    ListSubscriptionsByTopicRequest listRequest = new ListSubscriptionsByTopicRequest(topicArn);
    ListSubscriptionsByTopicResult listResult = snsClient.listSubscriptionsByTopic(listRequest);

    // Unsubscribe each subscription associated with the topic
    for (Subscription subscription : listResult.getSubscriptions()) {
      String subscriptionArn = subscription.getSubscriptionArn();
      UnsubscribeRequest unsubscribeRequest = new UnsubscribeRequest(subscriptionArn);
      snsClient.unsubscribe(unsubscribeRequest);
      System.out.println("Unsubscribed from subscription ARN: " + subscriptionArn);
    }

    // Delete the SQS queue and SNS topic
    sqsClient.deleteQueue(queueUrl);
    snsClient.deleteTopic(topicArn);
  }
}
