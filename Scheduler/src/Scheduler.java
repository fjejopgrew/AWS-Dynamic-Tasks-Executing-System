import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import org.json.JSONArray;
import org.json.JSONObject;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class Scheduler {

	public static AmazonSQS sqs;
	private static final String TASK_SQS_URL = "Your Task SQS URL";
	private static final Boolean USE_LOCAL_WORKER = false;
	private static final int SOCKET_TO_LISTEN_TO = 9090;
	
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

	private static void sendTasksMessageWithTasks(JSONArray tasks) {
		for(int i = 0; i < tasks.length(); i++) {
			JSONObject task = tasks.getJSONObject(i);
			sqs.sendMessage(new SendMessageRequest().withQueueUrl(TASK_SQS_URL).withMessageBody(task.toString()));
		}
	}

	public static void main(String[] args) throws IOException {
		
		if(USE_LOCAL_WORKER) {
			ServerSocket listener = new ServerSocket(SOCKET_TO_LISTEN_TO);
			int localWorkerID = 0;
			try {
				while (true) {
					Socket socket = listener.accept();
					System.out.println(socket.getRemoteSocketAddress());
					
					BufferedReader fromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String requestContent = fromClient.readLine();
					JSONArray tasksArray = new JSONArray(requestContent);
					
					PrintWriter response = new PrintWriter(socket.getOutputStream(), true);
					response.println("success");
					
					LocalWorker localWorker = new LocalWorker(localWorkerID, tasksArray);
					localWorker.start();
					localWorkerID++;
				}
			}
			finally {
				listener.close();
			}
			
		} else {
			init();
			ResponseHandler responseHandler = new ResponseHandler();
			responseHandler.start();
			
			ServerSocket listener = new ServerSocket(SOCKET_TO_LISTEN_TO);
			try {
				while (true) {
					Socket socket = listener.accept();
					BufferedReader fromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String requestContent = fromClient.readLine();
					JSONArray tasksArray = new JSONArray(requestContent);
					System.out.println(tasksArray);
					sendTasksMessageWithTasks(tasksArray);
				}
			}
			finally {
				listener.close();
			}
		}
	}
}
