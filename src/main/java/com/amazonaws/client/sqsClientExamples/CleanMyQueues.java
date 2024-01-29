package com.amazonaws.client.sqsClientExamples;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import java.util.List;

public class CleanMyQueues {

  private static final String REGION_IAD = "us-east-1";
  private static final String REGION_PDX = "us-west-2";

  private static void cleanMyQueues() {
    final AmazonSQS sqsClient = AmazonSQSClientBuilder.standard().withRegion(REGION_PDX).build();
    List<String> queueUrls = sqsClient.listQueues().getQueueUrls();
    for (String queueUrl : queueUrls) {
      sqsClient.deleteQueue(queueUrl);
    }
    System.out.println("All queues have been deleted.");
  }

  public static void main(String[] args) {
    cleanMyQueues();
  }
}
