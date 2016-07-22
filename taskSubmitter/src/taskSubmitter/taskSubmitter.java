package taskSubmitter;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.Scanner;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONObject;;

public class taskSubmitter {

	private static final String SERVER_ADDRESS = "IP of Scheduler";
	private static final int SERVER_SOCKET = 9090;
	private static final int PORT_TO_LISTEN_TO = 9191;
	private static int taskCounter;
	private static Scanner in;
	
	public static void main(String argv[]) throws Exception
	{
		in = new Scanner(System.in); 
		System.out.println("Please enter the task file name on the current direction:");
		String fileName = in.next();

		File file = new File(fileName);
		if(file.exists()) {
			Socket socket = new Socket(SERVER_ADDRESS, SERVER_SOCKET);
			String clientIP = getClientIP();
			BufferedOutputStream toServer =  new BufferedOutputStream(socket.getOutputStream());
			OutputStreamWriter outputStreamWriter = new OutputStreamWriter(toServer, "US-ASCII");

			JSONArray taskArray = new JSONArray();
			try(BufferedReader bufferReader = new BufferedReader(new FileReader(fileName))) {
				String line = new String();
				while (true) {
					line = bufferReader.readLine();
					if(line == null) {
						break;
					}
					JSONObject taskObject =new JSONObject();
					UUID taskID = UUID.randomUUID();
					taskObject.put("taskID", taskID);
					taskObject.put("taskContent", line);
					taskObject.put("clientIP", clientIP);
					taskObject.put("clientPort", PORT_TO_LISTEN_TO);

					taskArray.put(taskObject);
				}
				taskCounter = taskArray.length();
				outputStreamWriter.write(taskArray.toString());
				outputStreamWriter.flush();
				
				socket.close();
				listenToResponse();
			}
		} else {
			System.out.println("File not exist");
		}
	}

	private static void listenToResponse() throws IOException
	{
		ServerSocket listener = new ServerSocket(PORT_TO_LISTEN_TO);
		try {
			while (true) {
				Socket socket = listener.accept();
				System.out.println("Get response from Server with ip:" + socket.getRemoteSocketAddress());

				BufferedReader fromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String response = "Result:";
				while(response != null) {
					System.out.println(response);
					response = fromServer.readLine();
				}
				taskCounter--;
				if(taskCounter == 0) {
					System.out.println("All tasks has been finished. Stop");
					break;
				}
			}
		} finally {
			listener.close();
		}
	}
	
	public static String getClientIP() throws Exception {
		//This is for getting the public IP of the client, so the server can send responses
        URL ipChecker = new URL("http://checkip.amazonaws.com");
        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(
            		ipChecker.openStream()));
            String ip = in.readLine();
            return ip;
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
