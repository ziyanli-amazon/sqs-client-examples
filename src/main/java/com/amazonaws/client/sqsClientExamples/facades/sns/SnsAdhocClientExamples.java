package com.amazonaws.client.sqsClientExamples.facades.sns;

import static com.amazonaws.client.sqsClientExamples.Constants.REGION;

import java.util.UUID;

public class SnsAdhocClientExamples {

  private static final String topicName = UUID.randomUUID() + "-adHoc-Topic";
  private static final SnsFacades snsClient = SnsFacades.createNewClient(REGION, topicName);
  static String topicArn = snsClient.createTopic().getTopicArn();

  public static void main(String[] args) {
    System.out.print(topicArn);
  }
}
