package WorkerNodeAllocator;

import java.util.Scanner;

import org.apache.commons.codec.binary.Base64;

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

public class WorkerNodeAllocator {

	private static AmazonEC2 ec2;
	private static final String SECURITY_GROUP_ID = "Your Security Group Name";
	private static final String KEY_PAIR_NAME = "Key pair name";
	private static final String IMAGE_ID = "Your Image ID";
	private static Scanner in;
	
	public static void main(String argv[]) {
		init();
		
		in = new Scanner(System.in); 
		System.out.println("Please enter the number of work node to alloc:");
		int number = in.nextInt();
		
		for(int i = 0; i < number; i++) {
			createWorkerNode();
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
