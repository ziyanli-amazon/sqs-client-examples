package com.amazonaws.client.sqsClientExamples.facades.sns;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicResult;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.SetSubscriptionAttributesRequest;
import com.amazonaws.services.sns.model.SetSubscriptionAttributesResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sns.model.UnsubscribeResult;
import java.util.Map;

public class SnsFacades {
  AmazonSNS sns;
  String topicName;
  private String topicArn;

  SnsFacades(AmazonSNS amazonSNS, String topicName) {
    this.sns = amazonSNS;
    this.topicName = topicName;
  }

  public static SnsFacades createNewClient(String region, String topicName) {
    AmazonSNS snsClient = getClient(region);
    return new SnsFacades(snsClient, topicName);
  }

  private static AmazonSNS getClient(String region) {
    return AmazonSNSClientBuilder.standard().withRegion(region).build();
  }

  public CreateTopicResult createTopic() {
    CreateTopicResult result = sns.createTopic(topicName);
    topicArn = result.getTopicArn();
    return result;
  }

  public SubscribeResult subscribe(String protocol, String endpoint, Map<String, String> attributes) {
    SubscribeRequest request = new SubscribeRequest(topicArn, protocol, endpoint).withAttributes(attributes);
    return sns.subscribe(request);
  }

  public PublishResult publish(String message) {
    PublishRequest request = new PublishRequest(topicArn, message);
    return sns.publish(request);
  }

  public ListSubscriptionsByTopicResult listSubscriptionsByTopic() {
    ListSubscriptionsByTopicRequest request = new ListSubscriptionsByTopicRequest(topicArn);
    return sns.listSubscriptionsByTopic(request);
  }

  public UnsubscribeResult unsubscribe(String subscriptionArn) {
    UnsubscribeRequest request = new UnsubscribeRequest(subscriptionArn);
    return sns.unsubscribe(request);
  }

  public DeleteTopicResult deleteTopic() {
    return sns.deleteTopic(topicArn);
  }

  public SetSubscriptionAttributesResult setSubscriptionAttributes(String subscriptionArn, String attributeKey, String attributeValue) {
    SetSubscriptionAttributesRequest request = new SetSubscriptionAttributesRequest(subscriptionArn, attributeKey, attributeValue);
    return sns.setSubscriptionAttributes(request);
  }
}
