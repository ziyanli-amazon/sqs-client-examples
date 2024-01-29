package com.amazonaws.client.sqsClientExamples.facades.sqs;

import static com.amazonaws.client.sqsClientExamples.Constants.REGION;

import java.util.UUID;

public class SqsAdhocClientExamples {

  private static final String queueName = UUID.randomUUID() + "-adHoc-Queue";
  private static final SqsFacades sqsClient = SqsFacades.createNewClient(REGION, queueName);
  static String queueUrl = sqsClient.createQueue().getQueueUrl();

  public static void main(String[] args) {
    System.out.print(queueUrl);
  }
}
