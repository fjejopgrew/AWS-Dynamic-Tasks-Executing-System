
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import org.apache.commons.codec.binary.Base64;

public class SQSMoniter {

	private static AmazonEC2 ec2;
	public static AmazonSQS sqs;
	private static final int MAX_NUMBER_OF_NODES = 10;
	private static final String TASK_SQS_URL = "Your Task SQS URL";
	private static final String SECURITY_GROUP_ID = "Your Security Group ID";
	private static final String KEY_PAIR_NAME = "Key Name of Instance";
	private static final String IMAGE_ID = "Your Image ID";
	private static final Boolean DYNAMIC_PRIVISION = true;

	public static void main(String argv[]) throws Exception {		
		init();
				
		if(DYNAMIC_PRIVISION)
		{
			int preSQSLength = 0;
			while(true) {
				GetQueueAttributesResult attributeSQSLength = sqs.getQueueAttributes(new GetQueueAttributesRequest().withQueueUrl(TASK_SQS_URL).withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages));
				int currentSQSLength = Integer.parseInt(attributeSQSLength.getAttributes().get("ApproximateNumberOfMessages"));
				if(currentSQSLength == 0) {
					//No tasks wait 1 second and try again.
					Thread.sleep(1000);
				} else {
					if(currentSQSLength >= preSQSLength) {
						//If the queue length keep improving, or not decreasing, create one remote worker node to do the task, wait 60 seconds for the instance to boot, and check queue length again.
						createWorkerNode();
						Thread.sleep(1000 * 60);
					} else {
						//If the queue length is decreasing, just wait 10 seconds and check again.
						Thread.sleep(1000);
					}
				}
				preSQSLength = currentSQSLength;
			}
		} else {
			//This is for not using Dynamic provision, which means once there's some messages, start max number of worker nodes to do the tasks.

			while(true) {
				GetQueueAttributesResult attributeSQSLength = sqs.getQueueAttributes(new GetQueueAttributesRequest().withQueueUrl(TASK_SQS_URL).withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages));
				int currentSQSLength = Integer.parseInt(attributeSQSLength.getAttributes().get("ApproximateNumberOfMessages"));
				if(currentSQSLength == 0) {
					//No tasks wait 1 second and try again.
					Thread.sleep(1000);
				} else {
					//There are tasks, create max number of worker nodes to do the tasks
					for(int i = 0; i < MAX_NUMBER_OF_NODES; i++) {
						createWorkerNode();
					}
					Thread.sleep(600 * 1000);
					//Wait 10 minutes and check again.
				}

			}
		}
	}

	private static void init() {
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (/Users/UserName/.aws/credentials), and is in valid format.",
							e);
		}
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);

		sqs = new AmazonSQSClient(credentials);
		sqs.setRegion(usWest2);

		ec2 = new AmazonEC2Client(credentials);
		ec2.setRegion(usWest2);
	}

	private static void createWorkerNode() {
		  RunInstancesRequest runInstancesRequest = new RunInstancesRequest().withImageId(IMAGE_ID)
		  																	 .withInstanceType(InstanceType.C3Large)
		  																	 .withMinCount(1)
		  																	 .withMaxCount(1)
		  																	 .withKeyName(KEY_PAIR_NAME)
		  																	 .withSecurityGroupIds(SECURITY_GROUP_ID)
		  																	 .withUserData(Base64.encodeBase64String("#!/bin/bash\n cd home/ubuntu/\n java -jar ec2Worker.jar > log.txt".getBytes()));
		  RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);
		  System.out.println(runInstancesResult);
	}
	
	
}
