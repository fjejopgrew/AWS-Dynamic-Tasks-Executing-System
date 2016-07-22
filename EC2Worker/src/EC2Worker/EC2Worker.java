package EC2Worker;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.imageio.ImageIO;
import org.apache.commons.io.FileDeleteStrategy;
import org.json.JSONObject;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.xuggle.mediatool.IMediaWriter;
import com.xuggle.mediatool.ToolFactory;
import com.xuggle.xuggler.ICodec;

public class EC2Worker {

	private static AmazonSQS sqs;
	private static AmazonEC2 ec2;
    private static AmazonDynamoDBClient dynamoDB;
    private static AmazonS3Client s3;
	private static final String TASK_SQS_URL = "Your Task SQS URL";
	private static final String RESULT_SQS_URL = "Your Result SQS URL";
	private static final String DYNAMODB_TABLE_NAME = "Your Table Name";
	private static final String S3_BUCKET_NAME = "Your S3 Bucket Name";
	private static final String CREDENTIAL_ACCESSKEY = "Your Credential Access Key";
	private static final String CREDENTIAL_SECRETKEY = "Your Credential Secret Key";

	public static void main(String[] args) throws InterruptedException, IOException {
		System.out.println("Start");
		init();
		processMessages();
	}

	private static void init() {
		
		System.out.println("StartInit");
		AWSCredentials credentials = new BasicAWSCredentials(CREDENTIAL_ACCESSKEY, CREDENTIAL_SECRETKEY) ;

		Region usWest2 = Region.getRegion(Regions.US_WEST_2);

		sqs = new AmazonSQSClient(credentials);
		sqs.setRegion(usWest2);

		ec2 = new AmazonEC2Client(credentials);
		ec2.setRegion(usWest2);
		
        dynamoDB = new AmazonDynamoDBClient(credentials);
        dynamoDB.setRegion(usWest2);
        
        s3 = new AmazonS3Client(credentials);
        s3.setRegion(usWest2);
	}

	private static void processMessages() throws InterruptedException, IOException {
		try {
			
			System.out.println("StartProcessMessages");

			int idleTime = 0;

			while(true) {
				if(idleTime >= 120) {
					terminateCurrentWorkerNode();
					break;
				}
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(TASK_SQS_URL);
				List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				System.out.println("StartReceiveMessages");

				if(messages.size() < 1) {
					//If there's no message got, sleep 1 second and try again.
					Thread.sleep(1000);
					idleTime++;
					continue;
				} else {
					idleTime = 0;
					System.out.println("Receiving messages from SQS.");
					
					for (Message message : messages) {
						JSONObject task = new JSONObject(message.getBody());
						String taskID = task.getString("taskID");
						
						if(checkDynamoDB(taskID)) {
							//Duplicated message
							continue;
						} else {
							writeToDynamoDB(taskID);
						}
						String taskContent = task.getString("taskContent");
						String clientIP = task.getString("clientIP");
						int clientPort = task.getInt("clientPort");
						String[] commandAndParameters = taskContent.split(" ");

						if(commandAndParameters[0].equals("sleep")) {
							
							int timeToSleep = Integer.parseInt(commandAndParameters[1]);
							Thread.sleep(timeToSleep);
							System.out.println("Sleep:" + commandAndParameters[1]);
							String result = "Sleep" + timeToSleep + "finished";
							JSONObject resultObject =new JSONObject();
							resultObject.put("result", result);
							resultObject.put("clinetIP", clientIP);
							resultObject.put("clientPort", clientPort);
							
							pushResultToResultSQS(resultObject.toString());
							
						} else if(commandAndParameters[0].equals("images")) {
							System.out.println("transform images");
							String[] imageUrls = commandAndParameters[1].split(",");
							File folder = downloadImagesToLocal(imageUrls);
							transformToVideo(taskID+".mp4", folder.listFiles());
							URL urlInS3 = saveToS3(taskID+".mp4");
							String result = "Transform successfully, video url:" + urlInS3.toString();
							JSONObject resultObject =new JSONObject();
							resultObject.put("clinetIP", clientIP);
							resultObject.put("clientPort", clientPort);
							resultObject.put("result", result);
							pushResultToResultSQS(resultObject.toString());
						}
						// Delete a message
						System.out.println("Deleting a message.\n");
						String messageRecieptHandle = message.getReceiptHandle();
						sqs.deleteMessage(new DeleteMessageRequest(TASK_SQS_URL, messageRecieptHandle));
					}
				}
			}
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
					"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
					"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}
	
	private static Boolean checkDynamoDB(String taskID) {
        HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
        Condition condition = new Condition()
            .withComparisonOperator(ComparisonOperator.EQ)
            .withAttributeValueList(new AttributeValue().withS(taskID));
        scanFilter.put("taskID", condition);
        ScanRequest scanRequest = new ScanRequest(DYNAMODB_TABLE_NAME).withScanFilter(scanFilter);
        ScanResult scanResult = dynamoDB.scan(scanRequest);		
		return scanResult.getCount() != 0;//not equal to 0 means already exist.
	}
	
	private static void writeToDynamoDB(String taskID) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("taskID", new AttributeValue(taskID));
        PutItemRequest putItemRequest = new PutItemRequest(DYNAMODB_TABLE_NAME, item);
        dynamoDB.putItem(putItemRequest);
	}
	
	private static File downloadImagesToLocal(String[] imageUrls) throws IOException {
		int counter = 1;
		File folder = new File("img");
		if(!folder.exists()){
			folder.mkdir();
		}
		for (File file : folder.listFiles()) {
		    FileDeleteStrategy.FORCE.delete(file);
		}
		
		for(String imageUrl:imageUrls) {
			URL url = new URL(imageUrl);
			ReadableByteChannel rbc = Channels.newChannel(url.openStream());
			FileOutputStream fos = new FileOutputStream(".//img/img"+counter+".jpg");
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
			fos.close();
			rbc.close();
			counter++;
		}
		return folder;		
	}
	
	private static URL saveToS3(String fileName) throws InterruptedException {
		File videoFile = new File(fileName);
		s3.putObject(new PutObjectRequest(S3_BUCKET_NAME, fileName, videoFile).withCannedAcl(CannedAccessControlList.PublicRead));
		URL url = s3.getUrl(S3_BUCKET_NAME, fileName);
		System.out.println("Successfully upload, url:" + url);
		return url;
	}
	
	private static void transformToVideo(String videoName, File[] images) throws IOException {
		int numberOfImages = images.length;
		final IMediaWriter writer = ToolFactory.makeWriter(videoName);
		final double FRAME_RATE = 1.0;
		//long nextFrameTime = 0;
		writer.addVideoStream(0, 0,ICodec.ID.CODEC_ID_MPEG4,1920, 1080);
		long startTime = System.nanoTime(); 
		for(int i=0;i<numberOfImages;i++){
			URL url = images[i].toURI().toURL();
			BufferedImage img = ImageIO.read(url);
			int type = img.getType() == 0? BufferedImage.TYPE_INT_ARGB : img.getType();
			img = resizeImage(img, type);
			img = convertToType(img, BufferedImage.TYPE_3BYTE_BGR);
			writer.encodeVideo(0,img, System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
			try {
				Thread.sleep((long) (1000 / FRAME_RATE));
			}catch (InterruptedException e) {
			// ignore
			}
		}
		writer.close();
	}
	
	private static BufferedImage resizeImage(BufferedImage originalImage, int type) {
		BufferedImage image = new BufferedImage(300, 300, type);
		Graphics2D g = image.createGraphics();
		g.drawImage(originalImage, 0, 0, 300, 300, null);
		g.dispose();
		return image;
	}
	
	private static BufferedImage convertToType(BufferedImage sourceImage, int targetType) {
		BufferedImage image;
		// if the source image is already the target type, return the source image
		if (sourceImage.getType() == targetType) {
			image = sourceImage;
		}
		// otherwise create a new image of the target type and draw the new image
		else {
			image = new BufferedImage(sourceImage.getWidth(), sourceImage.getHeight(), targetType);
			image.getGraphics().drawImage(sourceImage, 0, 0, null);
		}
		return image;
	}
	
	
	private static void pushResultToResultSQS(String result) {
		sqs.sendMessage(new SendMessageRequest().withQueueUrl(RESULT_SQS_URL).withMessageBody(result));
	}
	
	private static void terminateCurrentWorkerNode() throws IOException {
		String instanceID = retrieveInstanceId();
		System.out.println("Idle time reach threshold, start terminate instance" + instanceID);
		TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest().withInstanceIds(instanceID);
		ec2.terminateInstances(terminateRequest);
	}

	private static String retrieveInstanceId() throws IOException {
		//This is for shutting down itself
		String EC2InstanceId = null;
		String inputLine;
		URL EC2MetaData = new URL("http://169.254.169.254/latest/meta-data/instance-id");
		URLConnection EC2MD = EC2MetaData.openConnection();
		BufferedReader in = new BufferedReader(new InputStreamReader(EC2MD.getInputStream()));
		while ((inputLine = in.readLine()) != null) {
			EC2InstanceId = inputLine;
		}
		in.close();
		return EC2InstanceId;
	}
}
