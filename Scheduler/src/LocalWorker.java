import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import org.json.JSONArray;
import org.json.JSONObject;


public class LocalWorker implements Runnable{
	private Thread t;
	private int workerNo;
	private JSONArray tasks;

	LocalWorker(int workerNo, JSONArray tasks) {
		this.tasks = tasks;
		this.workerNo = workerNo;
		System.out.println("Creating local worker" + workerNo);
	}

	@Override
	public void run() {
		for(int i = 0; i < tasks.length(); i++) {
			JSONObject task = tasks.getJSONObject(i);
			System.out.println("JSON:" + task);
			String clientIP = task.getString("clientIP");
			int clientPort = task.getInt("clientPort");
			String taskContent = task.getString("taskContent");
			String[] commandAndParameters = taskContent.split(" ");

			if(commandAndParameters[0].equals("sleep")) {
				try {
					int timeToSleep = Integer.parseInt(commandAndParameters[1]);
					Thread.sleep(timeToSleep);
					System.out.println("Sleep:" + commandAndParameters[1]);
					String result = "Sleep " + timeToSleep + " finished";
					System.out.println(result);
					sendResponseToClient(result, clientIP, clientPort);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void start () {
		System.out.println("Starting Worker" +  workerNo);
		if (t == null)
		{
			t = new Thread (this);
			t.start ();
		}
	}

	private void sendResponseToClient(String response, String clientIP, int clientPort) throws UnknownHostException, IOException {
		Socket socket = new Socket(clientIP, clientPort);
		BufferedOutputStream toClient =  new BufferedOutputStream(socket.getOutputStream());
		OutputStreamWriter outputStreamWriter = new OutputStreamWriter(toClient, "US-ASCII");
		outputStreamWriter.write(response);
		outputStreamWriter.flush();
		socket.close();		
	}
}
