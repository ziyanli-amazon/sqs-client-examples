package com.amazonaws.client.sqsClientExamples.facades.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AddPermissionRequest;
import com.amazonaws.services.sqs.model.AddPermissionResult;
import com.amazonaws.services.sqs.model.CancelMessageMoveTaskRequest;
import com.amazonaws.services.sqs.model.CancelMessageMoveTaskResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ListDeadLetterSourceQueuesRequest;
import com.amazonaws.services.sqs.model.ListDeadLetterSourceQueuesResult;
import com.amazonaws.services.sqs.model.ListMessageMoveTasksRequest;
import com.amazonaws.services.sqs.model.ListMessageMoveTasksResult;
import com.amazonaws.services.sqs.model.ListQueueTagsResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.PurgeQueueResult;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.RemovePermissionRequest;
import com.amazonaws.services.sqs.model.RemovePermissionResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesResult;
import com.amazonaws.services.sqs.model.StartMessageMoveTaskRequest;
import com.amazonaws.services.sqs.model.StartMessageMoveTaskResult;
import com.amazonaws.services.sqs.model.TagQueueRequest;
import com.amazonaws.services.sqs.model.TagQueueResult;
import com.amazonaws.services.sqs.model.UntagQueueRequest;
import com.amazonaws.services.sqs.model.UntagQueueResult;
import java.util.Map;

class SqsFacades {
  AmazonSQS sqs;
  String queueName;
  private String queueUrl;

  SqsFacades(AmazonSQS amazonSQS, String queueName) {
    this.sqs = amazonSQS;
    this.queueName = queueName;
  }

  public static SqsFacades createNewClient(String region, String queueName) {
    AmazonSQS sqsClient = getClient(region);
    return new SqsFacades(sqsClient, queueName);
  }

  private static AmazonSQS getClient(String region) {
    return AmazonSQSClientBuilder.standard().withRegion(region).build();
  }

  public CreateQueueResult createQueue() {
    CreateQueueRequest request = new CreateQueueRequest(queueName);
    CreateQueueResult result = sqs.createQueue(request);
    queueUrl = result.getQueueUrl();
    return result;
  }

  public DeleteQueueResult deleteQueue() {
    DeleteQueueRequest request = new DeleteQueueRequest(queueUrl);
    return sqs.deleteQueue(request);
  }

  public SendMessageResult sendMessage(String messageBody) {
    SendMessageRequest request = new SendMessageRequest(queueUrl, messageBody);
    return sqs.sendMessage(request);
  }

  public SendMessageResult sendMessage(
      String messageBody,
      String messageDeduplicationId,
      int delaySeconds,
      Map<String, MessageAttributeValue> messageAttributes,
      String messageGroupId) {
    SendMessageRequest request =
        new SendMessageRequest(queueUrl, messageBody)
            .withMessageDeduplicationId(messageDeduplicationId)
            .withDelaySeconds(delaySeconds)
            .withMessageAttributes(messageAttributes)
            .withMessageGroupId(messageGroupId);
    return sqs.sendMessage(request);
  }

  public SendMessageBatchResult sendMessageBatch(
      String messageBody,
      String messageDeduplicationId,
      int delaySeconds,
      Map<String, MessageAttributeValue> messageAttributes,
      String messageGroupId,
      String id) {
    SendMessageBatchRequest request = new SendMessageBatchRequest(queueUrl).withEntries();
    SendMessageBatchRequestEntry entry =
        new SendMessageBatchRequestEntry()
            .withId(id)
            .withMessageBody(messageBody)
            .withMessageDeduplicationId(messageDeduplicationId)
            .withDelaySeconds(delaySeconds)
            .withMessageAttributes(messageAttributes)
            .withMessageGroupId(messageGroupId);
    return sqs.sendMessageBatch(request);
  }

  public ReceiveMessageResult receiveMessage() {
    return sqs.receiveMessage(queueUrl);
  }

  public DeleteMessageResult deleteMessage(String receiptHandle) {
    DeleteMessageRequest request = new DeleteMessageRequest(queueUrl, receiptHandle);
    return sqs.deleteMessage(request);
  }

  public DeleteMessageBatchResult deleteMessageBatch(String receiptHandle) {
    DeleteMessageBatchRequest request = new DeleteMessageBatchRequest(queueUrl);
    return sqs.deleteMessageBatch(request);
  }

  public GetQueueAttributesResult getQueueAttributes() {
    GetQueueAttributesRequest request =
        new GetQueueAttributesRequest(queueUrl).withAttributeNames("All");
    return sqs.getQueueAttributes(request);
  }

  public SetQueueAttributesResult setQueueAttribute(Map<String, String> attributes) {
    SetQueueAttributesRequest request =
        new SetQueueAttributesRequest().withAttributes(attributes).withQueueUrl(queueUrl);
    return sqs.setQueueAttributes(request);
  }

  public PurgeQueueResult purgeQueue() {
    PurgeQueueRequest request = new PurgeQueueRequest(queueUrl);
    return sqs.purgeQueue(request);
  }

  public ChangeMessageVisibilityResult changeMessageVisibility(
      String receiptHandle, int visibilityTimeout) {
    ChangeMessageVisibilityRequest request =
        new ChangeMessageVisibilityRequest()
            .withQueueUrl(queueUrl)
            .withVisibilityTimeout(visibilityTimeout)
            .withReceiptHandle(receiptHandle);
    return sqs.changeMessageVisibility(request);
  }

  public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(
      String receiptHandle, int visibilityTimeout) {
    ChangeMessageVisibilityBatchRequestEntry entry =
        new ChangeMessageVisibilityBatchRequestEntry()
            .withId("id")
            .withReceiptHandle(receiptHandle)
            .withVisibilityTimeout(visibilityTimeout);
    ChangeMessageVisibilityBatchRequest request =
        new ChangeMessageVisibilityBatchRequest().withEntries(entry).withQueueUrl(queueUrl);
    return sqs.changeMessageVisibilityBatch(request);
  }

  public GetQueueUrlResult getQueueUrl(String queueName) {
    return sqs.getQueueUrl(queueName);
  }

  public ListQueuesResult listQueues() {
    return sqs.listQueues();
  }

  public ListDeadLetterSourceQueuesResult listDeadLetterSourceQueues(
      int maxResult, String nextToken) {
    ListDeadLetterSourceQueuesRequest request =
        new ListDeadLetterSourceQueuesRequest()
            .withQueueUrl(queueUrl)
            .withMaxResults(maxResult)
            .withNextToken(nextToken);
    return sqs.listDeadLetterSourceQueues(request);
  }

  public ListQueueTagsResult listQueueTags() {
    return sqs.listQueueTags(queueUrl);
  }

  public TagQueueResult tagQueue(Map<String, String> tags) {
    TagQueueRequest request = new TagQueueRequest().withQueueUrl(queueUrl).withTags(tags);
    return sqs.tagQueue(request);
  }

  public UntagQueueResult untagQueue(String tagKeys) {
    UntagQueueRequest request = new UntagQueueRequest().withQueueUrl(queueUrl).withTagKeys(tagKeys);
    return sqs.untagQueue(request);
  }

  public AddPermissionResult addPermission(String actions, String label) {
    AddPermissionRequest request = new AddPermissionRequest().withQueueUrl(queueUrl).withActions(actions).withLabel(label);
    return sqs.addPermission(request);
  }

  public RemovePermissionResult removePermission(String label) {
    RemovePermissionRequest request = new RemovePermissionRequest().withQueueUrl(queueUrl).withLabel(label);
    return sqs.removePermission(request);
  }

  public StartMessageMoveTaskResult startMessageMoveTask(
      String destinationArn, String sourceArn, int maxNumMsgPerSec) {
    StartMessageMoveTaskRequest request =
        new StartMessageMoveTaskRequest()
            .withDestinationArn(destinationArn)
            .withSourceArn(sourceArn)
            .withMaxNumberOfMessagesPerSecond(maxNumMsgPerSec);
    return sqs.startMessageMoveTask(request);
  }

  public ListMessageMoveTasksResult listMessageMoveTasks(int maxResult, String sourceArn) {
    ListMessageMoveTasksRequest request =
        new ListMessageMoveTasksRequest().withMaxResults(maxResult).withSourceArn(sourceArn);
    return sqs.listMessageMoveTasks(request);
  }

  public CancelMessageMoveTaskResult cancelMessageMoveTask(String taskHandle) {
    CancelMessageMoveTaskRequest request =
        new CancelMessageMoveTaskRequest().withTaskHandle(taskHandle);
    return sqs.cancelMessageMoveTask(request);
  }
}
