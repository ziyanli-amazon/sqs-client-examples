package com.amazonaws.client.sqsClientExamples;

import static com.amazonaws.client.sqsClientExamples.Constants.REGION;
import static com.amazonaws.client.sqsClientExamples.Utils.plainTextGenerator;
import static com.amazonaws.client.sqsClientExamples.Utils.stringBytesCalculator;

import com.amazonaws.client.sqsClientExamples.facades.sns.SnsFacades;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.CreateRoleRequest;
import com.amazonaws.services.identitymanagement.model.DeleteRoleRequest;
import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.GetRoleResult;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.amazonaws.services.identitymanagement.model.PutRolePolicyRequest;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.BufferingHints;
import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DeleteDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.ExtendedS3DestinationConfiguration;
import com.amazonaws.services.kinesisfirehose.model.ListDeliveryStreamsRequest;
import com.amazonaws.services.kinesisfirehose.model.ResourceInUseException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.AmazonSNSException;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.ListTopicsRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.util.IOUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SnsKDFirehoseJavaSDKv1ClientExamples {
  private static final String topicName = "SNS-Firehose-Topic";
  private static final String deliveryStreamName = "sns-firehose-delivery-stream";
  private static final String bucketName = "sns-firehose-delivery-stream-bucket";
  private static final String SNSFirehoseRoleName = "SNS-Firehose-Role";
  private static final String firehoseS3RoleName = "Firehose-S3-Role";
  static String topicArn;
  static String bucketArn;
  static String deliveryStreamArn;
  static String kmsArn;
  static String logStreamArn;
  static String lambdaFunctionArn;
  private static final SnsFacades snsClient = SnsFacades.createNewClient(REGION, topicName);

  public static void main(String[] args) throws InterruptedException {
    AmazonKinesisFirehose firehoseClient =
        AmazonKinesisFirehoseClientBuilder.standard().withRegion(REGION).build();
    AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(REGION).build();
    AmazonIdentityManagement iamClient =
        AmazonIdentityManagementClientBuilder.standard().withRegion(REGION).build();

    /** Step 0: Create SNS topic */
    topicArn = snsClient.createTopic().getTopicArn();
    System.out.println("Topic has been created: " + topicArn);

    /**
     * Step 1: a. Create S3 bucket as the destination for the Kinesis Firehose delivery stream; b.
     * Create an IAM role with firehose granting permission to s3, so firehose data stream can be
     * written into an s3 object; c. Configure s3 buffer conditions; d. Configure
     * ExtendedS3DestinationConfiguration for firehose data stream.
     */
    if (s3Client.doesBucketExistV2(bucketName)) {
      System.out.println("Bucket already exists. Skipping creating bucket...");
    } else {
      try {
        s3Client.createBucket(bucketName);
      } catch (AmazonS3Exception e) {
        System.err.println(e.getErrorMessage());
      }
    }
    bucketArn = "arn:aws:s3:::" + bucketName;
    System.out.println("bucket Arn: " + bucketArn);

    // Create IAM role with firehose granting permission to s3 so firehose data stream can be
    // written into an s3 object.
    String firehoseS3RoleArn =
        createIamRole(
            iamClient, firehoseS3RoleName, getFirehoseAssumedRoleDoc(), getFirehoseS3Policy());

    // Configure S3 buffer conditions to be: For buffer size, 1MBs; for Buffer interval, 60 MBs
    BufferingHints bufferingHints =
        new BufferingHints()
            .withSizeInMBs(1) // Buffer size in MBs
            .withIntervalInSeconds(60);

    // set up SNS destination
    ExtendedS3DestinationConfiguration s3DestinationConfiguration =
        new ExtendedS3DestinationConfiguration()
            .withBucketARN(bucketArn)
            .withRoleARN(firehoseS3RoleArn)
            .withBufferingHints(bufferingHints)
            .withCompressionFormat(CompressionFormat.UNCOMPRESSED);
    /**
     * Step2: Create firehose data stream with the ExtendedS3DestinationConfiguration defined
     * previously.
     */
    ListDeliveryStreamsRequest listDeliveryStreamsRequest = new ListDeliveryStreamsRequest();
    List<String> deliveryStreamNames =
        firehoseClient.listDeliveryStreams(listDeliveryStreamsRequest).getDeliveryStreamNames();

    CreateDeliveryStreamRequest createDeliveryStreamRequest =
        new CreateDeliveryStreamRequest()
            .withDeliveryStreamName(deliveryStreamName)
            .withExtendedS3DestinationConfiguration(s3DestinationConfiguration);
    // Create the delivery stream with the destination being the s3 bucket created previously
    if (!deliveryStreamNames.contains(deliveryStreamName)) {
      try {
        firehoseClient.createDeliveryStream(createDeliveryStreamRequest);
      } catch (ResourceInUseException e) {
        System.out.println(e.getErrorMessage());
      }
    } else {
      System.out.println("Skipping creating firehose dalivery data stream...");
    }

    // Describe the delivery stream to fetch the streamArn
    DescribeDeliveryStreamRequest describeDeliveryStreamRequest =
        new DescribeDeliveryStreamRequest().withDeliveryStreamName(deliveryStreamName);
    DescribeDeliveryStreamResult describeDeliveryStreamResult =
        firehoseClient.describeDeliveryStream(describeDeliveryStreamRequest);
    deliveryStreamArn =
        describeDeliveryStreamResult.getDeliveryStreamDescription().getDeliveryStreamARN();
    System.out.println("Delivery Stream arn: " + deliveryStreamArn);

    String deliveryStreamStatus =
        describeDeliveryStreamResult.getDeliveryStreamDescription().getDeliveryStreamStatus();
    System.out.println("The firehose data stream is in status: " + deliveryStreamStatus);

    /**
     * Please allow 15-30 min for the firehose delivery data stream to be created. Only perform the
     * follwoing actions when the delivery data stream's status is -Active-.
     */
    if (!deliveryStreamStatus.equals("ACTIVE")) {
      System.out.println("Firehose data stream is not active. Exiting...");
      return;
    }

    /**
     * Step 3: a. Create another IAM role, which grants access for SNS to call respective firehose
     * APIs like PutRecord, etc. b. Subscribe firehose data stream created previously to the SNS
     * topic
     */

    // Create an IAM role to grant SNS access to firehose resources.
    String SNSFirehoseRoleArn =
        createIamRole(
            iamClient,
            SNSFirehoseRoleName,
            getSnsAssumedRoleDoc(),
            getSnsFirehosePolicy(deliveryStreamArn));

    // Building the topic attribute like rawMessageEnable, SubscriptionRoleArn, etc..
    Map<String, String> attributes = new HashMap<>();
    attributes.put("RawMessageDelivery", "true");
    attributes.put("SubscriptionRoleArn", SNSFirehoseRoleArn);

    // Subscribe the Kinesis Firehose delivery stream to the SNS topic
    SubscribeResult subscription = snsClient.subscribe("firehose", deliveryStreamArn, attributes);

    if (subscription.getSdkHttpMetadata().getHttpStatusCode() == 200) {
      System.out.println("Subscription Successful!");
    }
    String subscriptionArn = subscription.getSubscriptionArn();

    /**
     * Step 4: a. Publish a message to SNS topic Supposedly, this message will be transferred via
     * firehose data stream to S3 bucket created previously. b. Verify that we got the message in S3
     * bucket.
     */
    String messageBody = plainTextGenerator(256000, true);
    int messageBodySize = stringBytesCalculator(messageBody);
    // System.out.println("Message Body is: " + messageBody);
    System.out.println("Message Body Size is: " + messageBodySize);
    try {
      snsClient.publish(messageBody);
    } catch (AmazonSNSException e) {
      System.err.println(e.getErrorMessage());
      System.exit(1);
    }
    System.out.println("Message published successfully!");

    Thread.sleep(61000);
    System.out.println(
        "Message received in S3 bucket?  ------> "
            + dataValidator(s3Client, bucketName, messageBody));
  }

  private static String getSnsFirehosePolicy(String streamArn) {
    return "{\n"
        + "  \"Version\": \"2012-10-17\",\n"
        + "  \"Statement\": [\n"
        + "    {\n"
        + "      \"Action\": [\n"
        + "        \"firehose:DescribeDeliveryStream\",\n"
        + "        \"firehose:ListDeliveryStreams\",\n"
        + "        \"firehose:ListTagsForDeliveryStream\",\n"
        + "        \"firehose:PutRecord\",\n"
        + "        \"firehose:PutRecordBatch\"\n"
        + "      ],\n"
        + "      \"Resource\": [\n"
        + "        \""
        + streamArn
        + "\"\n"
        + "      ],\n"
        + "      \"Effect\": \"Allow\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }

  private static String getFirehoseS3Policy() {
    return "{"
        + "    \"Version\": \"2012-10-17\","
        + "    \"Statement\": ["
        + "        {"
        + "            \"Effect\": \"Allow\","
        + "            \"Action\": ["
        + "                \"s3:AbortMultipartUpload\","
        + "                \"s3:GetBucketLocation\","
        + "                \"s3:GetObject\","
        + "                \"s3:ListBucket\","
        + "                \"s3:ListBucketMultipartUploads\","
        + "                \"s3:PutObject\""
        + "            ],"
        + "            \"Resource\": \""
        + "*"
        + "\""
        + "        },"
        + "        {"
        + "            \"Effect\": \"Allow\","
        + "            \"Action\": ["
        + "                \"kinesis:DescribeStream\","
        + "                \"kinesis:GetShardIterator\","
        + "                \"kinesis:GetRecords\","
        + "                \"kinesis:ListShards\""
        + "            ],"
        + "            \"Resource\": \""
        + "*"
        + "\""
        + "        }"
        + "    ]"
        + "}";
  }

  private static String getFirehoseAssumedRoleDoc() {
    return "{\n"
        + "    \"Version\": \"2012-10-17\",\n"
        + "    \"Statement\": [\n"
        + "        {\n"
        + "            \"Effect\": \"Allow\",\n"
        + "            \"Principal\": {\n"
        + "                \"Service\": \"firehose.amazonaws.com\"\n"
        + "            },\n"
        + "            \"Action\": \"sts:AssumeRole\"\n"
        + "        }\n"
        + "    ]\n"
        + "}";
  }

  private static String getSnsAssumedRoleDoc() {
    return "{\n"
        + "    \"Version\": \"2012-10-17\",\n"
        + "    \"Statement\": [\n"
        + "        {\n"
        + "            \"Effect\": \"Allow\",\n"
        + "            \"Principal\": {\n"
        + "                \"Service\": \"sns.amazonaws.com\"\n"
        + "            },\n"
        + "            \"Action\": \"sts:AssumeRole\"\n"
        + "        }\n"
        + "    ]\n"
        + "}";
  }

  private static String createIamRole(
      AmazonIdentityManagement iamClient,
      String iamRoleName,
      String assumedRolePolicyDoc,
      String rolePolicyDoc) {
    GetRoleRequest getRoleRequest = new GetRoleRequest().withRoleName(iamRoleName);
    try {
      // Attempt to get the IAM role
      GetRoleResult getRoleResult = iamClient.getRole(getRoleRequest);
      System.out.println("IAM role '" + iamRoleName + "' exists. Skipping...");
      return getRoleResult.getRole().getArn();
    } catch (NoSuchEntityException e) {
      System.out.println(
          "IAM role '" + iamRoleName + "' does not exist. Continue to create one for it...");
    }

    // Create IAM role with firehose granting permission to s3 so firehose data stream can be
    // written into an s3 object.
    CreateRoleRequest createRoleRequest =
        new CreateRoleRequest()
            .withRoleName(iamRoleName)
            .withAssumeRolePolicyDocument(assumedRolePolicyDoc);
    String roleArn = iamClient.createRole(createRoleRequest).getRole().getArn();
    // attach the policy to the role
    PutRolePolicyRequest putRolePolicyRequest =
        new PutRolePolicyRequest()
            .withRoleName(iamRoleName)
            .withPolicyName(iamRoleName)
            .withPolicyDocument(rolePolicyDoc);
    iamClient.putRolePolicy(putRolePolicyRequest);

    System.out.println(iamRoleName + " has been created, with role arn being " + roleArn);
    return roleArn;
  }

  private static void deleteResources(
      AmazonSNS snsClient,
      AmazonIdentityManagement iamClient,
      AmazonKinesisFirehose firehoseClient) {
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
    // Delete SNS topic
    snsClient.deleteTopic(topicArn);
    System.out.println("SNS topic has been deleted.");

    // Delete IAM roles
    DeleteRoleRequest deleteRolePolicyRequest =
        new DeleteRoleRequest().withRoleName(SNSFirehoseRoleName);
    iamClient.deleteRole(deleteRolePolicyRequest);
    System.out.println("IAM role has been deleted.");

    // Delete Kinesis Firehose delivery stream
    DeleteDeliveryStreamRequest deleteDeliveryStreamRequest =
        new DeleteDeliveryStreamRequest().withDeliveryStreamName(deliveryStreamName);
    firehoseClient.deleteDeliveryStream(deleteDeliveryStreamRequest);
  }

  private static boolean dataValidator(AmazonS3 s3Client, String bucketName, String messageBody) {
    ObjectListing objectListing = s3Client.listObjects(bucketName);
    List<String> keys = new ArrayList<>();
    for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
      System.out.println("Object key: " + objectSummary.getKey());
      System.out.println("Object size: " + objectSummary.getSize());
      keys.add(objectSummary.getKey());
    }

    // Verify that we got the message in S3 bucket.
    List<String> contents = new ArrayList<>();
    for (String key : keys) {
      GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
      try {
        S3Object s3Object = s3Client.getObject(getObjectRequest);
        // Get the content of the object as an input stream
        InputStream objectContent = s3Object.getObjectContent();
        // Read the content of the object
        String content = IOUtils.toString(objectContent);
        contents.add(content);
        // Close the input stream
        objectContent.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return contents.contains(messageBody);
  }
}

//    private static String getFirehoseS3Policy() {
//        return "{\n" +
//                "    \"Version\": \"2012-10-17\",\n" +
//                "    \"Statement\": [\n" +
//                "        {\n" +
//                "            \"Effect\": \"Allow\",\n" +
//                "            \"Action\": [\n" +
//                "                \"s3:AbortMultipartUpload\",\n" +
//                "                \"s3:GetBucketLocation\",\n" +
//                "                \"s3:GetObject\",\n" +
//                "                \"s3:ListBucket\",\n" +
//                "                \"s3:ListBucketMultipartUploads\",\n" +
//                "                \"s3:PutObject\"\n" +
//                "            ],\n" +
//                "            \"Resource\": [\n" +
//                "                \""+ bucketArn + "\",\n" +
//                "                \"" + bucketArn +"/*\"\n" +
//                "            ]\n" +
//                "        },\n" +
//                "        {\n" +
//                "            \"Effect\": \"Allow\",\n" +
//                "            \"Action\": [\n" +
//                "                \"kinesis:DescribeStream\",\n" +
//                "                \"kinesis:GetShardIterator\",\n" +
//                "                \"kinesis:GetRecords\",\n" +
//                "                \"kinesis:ListShards\"\n" +
//                "            ],\n" +
//                "            \"Resource\": \"" + deliveryStreamArn + "\"\n" +
//                "        },\n" +
//                "        {\n" +
//                "            \"Effect\": \"Allow\",\n" +
//                "            \"Action\": [\n" +
//                "                \"kms:Decrypt\",\n" +
//                "                \"kms:GenerateDataKey\"\n" +
//                "            ],\n" +
//                "            \"Resource\": [\n" +
//                "                \"" + kmsArn + "\"\n" +
//                "            ],\n" +
//                "            \"Condition\": {\n" +
//                "                \"StringEquals\": {\n" +
//                "                    \"kms:ViaService\": \"s3." + REGION + ".amazonaws.com\"\n" +
//                "                },\n" +
//                "                \"StringLike\": {\n" +
//                "                    \"kms:EncryptionContext:aws:s3:arn\": \"" + bucketArn +
// "\"\n" +
//                "                }\n" +
//                "            }\n" +
//                "        },\n" +
//                "        {\n" +
//                "            \"Effect\": \"Allow\",\n" +
//                "            \"Action\": [\n" +
//                "                \"logs:PutLogEvents\"\n" +
//                "            ],\n" +
//                "            \"Resource\": [\n" +
//                "                \""+ logStreamArn +"\"\n" +
//                "            ]\n" +
//                "        },\n" +
//                "        {\n" +
//                "            \"Effect\": \"Allow\",\n" +
//                "            \"Action\": [\n" +
//                "                \"lambda:InvokeFunction\",\n" +
//                "                \"lambda:GetFunctionConfiguration\"\n" +
//                "            ],\n" +
//                "            \"Resource\": [\n" +
//                "                \"" + lambdaFunctionArn + "\"\n" +
//                "            ]\n" +
//                "        }\n" +
//                "    ]\n" +
//                "}";
//    }
