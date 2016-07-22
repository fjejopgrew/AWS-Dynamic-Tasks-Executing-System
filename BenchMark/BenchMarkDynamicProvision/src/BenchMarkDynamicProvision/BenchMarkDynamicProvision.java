package BenchMarkDynamicProvision;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.UUID;
import org.json.JSONObject;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class BenchMarkDynamicProvision {
	public static AmazonSQS sqs;
	private static final String TASK_SQS_URL = "Your Task SQS URL";
	private static final String RESULT_SQS_URL = "Your Result SQS URL";
	
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
		sqs = new AmazonSQSClient(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		sqs.setRegion(usWest2);
	}
	
	public static void main(String[] args) {
		init();

		JSONObject taskObject =new JSONObject();
		UUID taskID = UUID.randomUUID();
		taskObject.put("taskID", taskID);
		taskObject.put("taskContent", "sleep 1");
		taskObject.put("clientIP", "benchmark test");
		taskObject.put("clientPort", 9191);
		
		String timeStamp = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(Calendar.getInstance().getTime());
		System.out.println("Start time:" + timeStamp);
		sqs.sendMessage(new SendMessageRequest().withQueueUrl(TASK_SQS_URL).withMessageBody(taskObject.toString()));
		
		while(true) {
			GetQueueAttributesResult attributeSQSLength = sqs.getQueueAttributes(new GetQueueAttributesRequest().withQueueUrl(RESULT_SQS_URL).withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages));
			int resultSQSLength = Integer.parseInt(attributeSQSLength.getAttributes().get("ApproximateNumberOfMessages"));
			if(resultSQSLength == 0) {
				//haven't finished
				continue;
			} else {
				//Finished
				timeStamp = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(Calendar.getInstance().getTime());
				System.out.println("Finished time:" + timeStamp);
				break;
			}
		}
	}
}
