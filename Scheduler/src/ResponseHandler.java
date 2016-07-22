import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.List;
import org.json.JSONObject;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;


public class ResponseHandler implements Runnable {
	
	private Thread t;
	public static AmazonSQS sqs;
	private static final String RESULT_SQS_URL = "Your Result SQS URL";

	ResponseHandler() {
		t = new Thread (this);
	}
	
	public void start () {
		t.start();
	}
	
	@Override
	public void run() {
		init();
		while(true) {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(RESULT_SQS_URL);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

			if(messages.size() < 1) {
				try {
					Thread.sleep(1000);
					continue;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			} else {
				for (Message message : messages) {
					JSONObject resultObj = new JSONObject(message.getBody());
					String result = resultObj.getString("result");
					String clientIP = resultObj.getString("clinetIP");
					int clientPort = resultObj.getInt("clientPort");
					System.out.println("Start Sending");
					Responser responser = new Responser(result, clientIP, clientPort);
					responser.start();
					System.out.println("Finish Sending");
					
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
		sqs = new AmazonSQSClient(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_2);
		sqs.setRegion(usWest2);
	}
		
	class Responser implements Runnable{

		private Thread rt;
		private String response;
		private String clientIP;
		private int clientPort;
		
		
		Responser(String r, String cIP, int cPort) {
			response = r;
			clientIP = cIP;
			clientPort = cPort;
			rt = new Thread (this);
		}
		
		public void start() {
			rt.start();
		}
		
		@Override
		public void run() {
			Socket socket;
			try {
				socket = new Socket(clientIP, clientPort);
				BufferedOutputStream toClient =  new BufferedOutputStream(socket.getOutputStream());
				OutputStreamWriter outputStreamWriter = new OutputStreamWriter(toClient, "US-ASCII");
				outputStreamWriter.write(response);
				outputStreamWriter.flush();
				socket.close();	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
