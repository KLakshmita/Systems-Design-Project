import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream; 
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Scanner;

public class BNameServer {

	public static final String LOOKUP_COMMAND = "lookup";
	public static final String INSERT_COMMAND = "insert";
	public static final String DELETE_COMMAND = "delete";

	public static final String ENTER_COMMAND = "enter";
	public static final String EXIT_COMMAND = "exit"; 
	public static final String PRED_EXIT_COMMAND = "predExit";  
	public static final String SUCC_EXIT_COMMAND = "succExit";  

	public static final String PRINT_COMMAND = "print";
	public static final String ASK_RANGE_COMMAND = "askRange";
	public static final String GET_DATA_COMMAND = "getData";
	public static final String UPDATE_INFO_COMMAND = "updateInfo";

	public static final String QUERY_COMMAND = "query";
	public static final String QUERY_RESPONSE = "queryResponse";

	public static final String KEY_NOT_FOUND_MESSAGE = "Key not found in ";
	public static final String KEY_FOUND_MESSAGE = "Key found in ";
	public static final String KEY_INSERTED_MESSAGE = "Key is successfully inserted in ";
	public static final String KEY_DELETED_MESSAGE = "Key is successfully deleted from "; 
	
	public static final String SUCCESSFUL_ENTERY_MESSAGE = "Successful Entry";
	public static final String SUCCESSFUL_EXIT_MESSAGE = "Successful Exit";

	private static int[] dataRange = {0, 1023};
	private static HashMap<Integer, String> data = new HashMap<Integer, String>();

	private static int myID, predID, succID;
	private static String myIP, predIP, succIP;
	private static int myPort, predPort, succPort; 
	private static ServerSocket serverSocket;

	public static void main(String[] args) throws IOException {

		String fileName = args[0];
		FileInputStream fstream = new FileInputStream(fileName);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream)); 

		myID = Integer.parseInt(br.readLine());
		myPort = Integer.parseInt(br.readLine());
		myIP = predIP = succIP = "localhost";
		predPort = succPort = myPort;
		predID = succID = myID;

		System.out.println("ID = " + myID + ", Port = " + myPort); 

		// fill the data hashmap
		String line = null; 
		while ((line = br.readLine()) != null) {
			String[] lineSplit = line.split(" ");
			int dataID = Integer.parseInt(lineSplit[0]);
			String dataValue = lineSplit[1];
			data.put(dataID, dataValue);
			System.out.println(dataID + " " + dataValue); 
		}  

		System.out.println("\nBootstrap Name Server is running .."); 
		serverSocket = new ServerSocket(myPort);

		Thread ThreadA = new ThreadA();
		ThreadA.start();

		Thread ThreadB = new ThreadB();
		ThreadB.start();

	}

	static class ThreadA extends Thread {  
		private Socket socket;
		private DataInputStream dis;
		private DataOutputStream dos;
		private Scanner scanner;


		public ThreadA() {

		}

		@Override
		public void run() {
			while (true) {
				scanner = new Scanner(System.in);  

				String command = null;
				String keyStr = null;
				String value = null;
				int key = -1;
				int commandLength;
				System.out.print("\n> ");
				command = scanner.nextLine();
				
				
				if (command.contains(" ")) {
					String[] splittedCommand = command.split(" ");
					commandLength = splittedCommand.length;
					command = splittedCommand[0];
					if (commandLength > 1) {
						keyStr = splittedCommand[1];
						key = Integer.valueOf(keyStr);
					}
					if (commandLength == 3) 
						value = splittedCommand[2];
				}

				switch (command) {
				case LOOKUP_COMMAND:
					findQueryRange(command, key, "");
					lookUp(command, key);
					break;
				case INSERT_COMMAND:
					findQueryRange(command, key, value);
					insert(command, key, value);
					break;
				case DELETE_COMMAND:
					findQueryRange(command, key, "");
					delete(command, key);
					break;
				case PRINT_COMMAND:
					System.out.println("Name Server Update: succID: " + succID + ", predID: " + predID + ", succIP: " + succIP + ", predIP: " + predIP);
					System.out.println("Name Server Update: succPort: " + succPort + ", predPort: " + predPort + ", startRange: " + dataRange[0] + ", endRange: " + dataRange[1]);
					printData();
					break;
				default:
					System.out.println("*Invalid Input .."); 
					break;
				}
			}
		}

		private void findQueryRange(String command, int key, String value) {
			if(key >= dataRange[0] && key <= dataRange[1]) {
				String Message = "";
				String IDs = "   ID: "+ myID +"\n";
				if(command.equals(INSERT_COMMAND)) {
					data.put(key, value);  
					Message = KEY_INSERTED_MESSAGE + myID;
				} // lookup and delete
				else if (data.containsKey(key)) {
					if(command.equals(LOOKUP_COMMAND)) {
						Message = KEY_FOUND_MESSAGE + myID + ": " + data.get(key);
					}else if(command.equals(DELETE_COMMAND)) {
						data.remove(key);
						Message = KEY_DELETED_MESSAGE + myID;
					}
					
				}// key in range but not found
				else 
					Message = KEY_NOT_FOUND_MESSAGE + myID;

				System.out.println("\n"); 
				System.out.println("1) " + Message);  				 
				System.out.println("2) Traversed Servers:"); 
				System.out.print(IDs);  
				System.out.println("\n"); 

			}else {
				try {
					Socket socket = new Socket(succIP, succPort);
					DataInputStream dis = new DataInputStream(socket.getInputStream());
					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					dos.writeUTF(QUERY_COMMAND);

					dos.writeUTF(command);
					dos.writeInt(key);
					dos.writeUTF(value);
					dos.writeUTF("   ID: "+ myID +"\n"); 

					socket.close();
					dis.close();
					dos.close();

				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
		private void lookUp(String command, int key) {
			
		}

		private void insert(String command, int key, String value) {
			
		}

		private void delete(String command, int key) {
			
		}
	}

	
	static class ThreadB extends Thread {  
		private Socket socket;
		private DataInputStream dis;
		private DataOutputStream dos;

		public ThreadB() { 

		}

		@Override
		public void run() {

			try {
				while (true) {
					socket = null;
					socket = serverSocket.accept();

					
					dis = new DataInputStream(socket.getInputStream());
					dos = new DataOutputStream(socket.getOutputStream());
					startProccess();
				}
			} catch (IOException e) {
				
				e.printStackTrace();
			}

		} 
		private void startProccess() throws IOException {
			String command = null; 

			command = dis.readUTF();

			switch (command) {
			case ENTER_COMMAND:
				System.out.println("New connection accepted"); 
				enter();
				System.out.print("> ");
				break; 
			case GET_DATA_COMMAND:
				System.out.println("Sending Data "); 
				giveDataEntery();
				System.out.print("> ");
				break;
			case UPDATE_INFO_COMMAND:
				System.out.println("Information Updated"); 
				UpdateInfoEntery();
				 
				System.out.print("> ");
				break;
			case PRED_EXIT_COMMAND:
				System.out.println("Predecessor Exiting"); 
				System.out.println("Getting Data "); 
				predExit();
				
				System.out.print("> ");
				break;
			case SUCC_EXIT_COMMAND:
				System.out.println("Successor Exited"); 
				succExit();
				System.out.print("> ");
				break; 
			case QUERY_RESPONSE:
				System.out.println("Receiving Query Response");  
				queryResponse();
				System.out.print("> ");
				break;
			default:
				System.out.println("Invalid Input"); 
				break;
			}

			socket.close();
			dis.close();
			dos.close();
		}

		private void queryResponse() {
			
			try {

				String Message = dis.readUTF();
				String IDs = dis.readUTF(); 
				System.out.println("\n"); 
				System.out.println("1) " + Message);  				 
				System.out.println("2) Traversed Servers:"); 
				System.out.print(IDs);  
				System.out.println("\n"); 
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void enter() {
			int newID = 0, newPort = 0;
			String newIP = null;

			try {

				newID = dis.readInt();
				newIP = dis.readUTF();
				newPort = dis.readInt();
			} catch (IOException e) {
				e.printStackTrace();
			}

			if(dataRange[0] == 0 && dataRange[1] == 1023)
				enterOnlyBns(newID, newIP, newPort);
			else 
				findSpotNS(newID, newIP, newPort);
		}

		private void enterOnlyBns(int newID, String newIP, int newPort) { 
			try { 
				System.out.println("newID: " + newID + ", newIP: " + newIP + ", newPort: " + newPort);
				System.out.println("Bns is the only one present");

				int newStartRange = dataRange[0], newEndRange = newID - 1;
				succID = newID;
				predID = newID;
				succIP = newIP;
				predIP = newIP;
				succPort = newPort;
				predPort = newPort;
				dataRange[0] = newID;
				dataRange[1] = 1023;

				dos.writeInt(myID);
				dos.writeInt(myID);
				dos.writeUTF(myIP);
				dos.writeUTF(myIP);
				dos.writeInt(myPort);
				dos.writeInt(myPort);
				dos.writeInt(newStartRange);
				dos.writeInt(newEndRange);

				dos.writeUTF("   ID: "+ myID +"\n");  
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void findSpotNS(int newID, String newIP, int newPort) {
			try { 
				System.out.println("newID: " + newID + ", newIP: " + newIP + ", newPort: " + newPort);
				System.out.println("Bns is looking for a spot ..");

				boolean found = false;
				String IDs;
				int newPredPort = 0, newSuccPort = 0, nextPort = succPort;
				String newPredIP = "", newSuccIP = "", nextIP = succIP;
				int newPredID = 0, newSuccID = 0, nextID = succID;

				IDs = "   ID: "+ myID +"\n";
				int newStartRange = 0, newEndRange = 0;


				while(!found) {
					IDs += "   ID: "+ nextID +"\n";
					Socket tSocket = new Socket(nextIP, nextPort);
					DataInputStream tDis = new DataInputStream(tSocket.getInputStream());
					DataOutputStream tDos = new DataOutputStream(tSocket.getOutputStream());

					tDos.writeUTF(ASK_RANGE_COMMAND);
					tDos.writeInt(newID);
					found = tDis.readBoolean();

					if(found) {
						newSuccID =  nextID;
						newSuccIP = nextIP;
						newSuccPort = nextPort;

						newPredID = tDis.readInt();
						newPredIP = tDis.readUTF();
						newPredPort = tDis.readInt(); 

						tSocket.close();
						tDis.close();
						tDos.close();

						break;
					}
					else { 
						nextID = tDis.readInt();
						nextIP = tDis.readUTF();
						nextPort = tDis.readInt();
					}

					if(nextID == myID) {

						newSuccID = myID;
						newSuccIP = myIP;
						newSuccPort = myPort;

						newPredID = predID;
						newPredIP = predIP;
						newPredPort = predPort; 

						tSocket.close();
						tDis.close();
						tDos.close();
						found = true;
					}
				}

				if(!found) {
					System.out.println("Couldn't find");
					return;
				}

				newStartRange = newPredID;
				newEndRange = newID - 1;

				dos.writeInt(newSuccID);
				dos.writeInt(newPredID);
				dos.writeUTF(newSuccIP);
				dos.writeUTF(newPredIP);
				dos.writeInt(newSuccPort);
				dos.writeInt(newPredPort);
				dos.writeInt(newStartRange);
				dos.writeInt(newEndRange);

				dos.writeUTF(IDs); 

				System.out.println("\n");
				System.out.println("Found an ID "+ newPredID +" and "+newSuccID+" ");
				System.out.println("\n");

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void predExit() {
			try {
				predID = dis.readInt();
				predIP = dis.readUTF();
				predPort = dis.readInt();
				dataRange[0] = dis.readInt();  

				String getData = dis.readUTF();
				if(!getData.equals("")) {
					String[] splitData = getData.split(" "); 
					int key;
					String value;
					for(int i = 0; i < splitData.length ; i++) {
						key = Integer.valueOf(splitData[i]);
						i++;
						value = splitData[i];
						data.put(key, value);
					}
				}
				printData();
				System.out.println("New predecessor is: " + predID);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void succExit() {
			try {
				succID = dis.readInt();
				succIP = dis.readUTF();
				succPort = dis.readInt(); 
				System.out.println("New successor is: " + succID);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void UpdateInfoEntery() {
			try {
				succID = dis.readInt();
				succIP = dis.readUTF();
				succPort = dis.readInt();
				System.out.println("New successor is: " + succID);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void giveDataEntery() {
			int[] newDataRange = new int[2];
			try {
				newDataRange[0] = dis.readInt();
				newDataRange[1] = dis.readInt();

				predID = dis.readInt();
				predIP = dis.readUTF();
				predPort = dis.readInt();


				String sendData = "";
				for(int i = newDataRange[0]; i <= newDataRange[1] ; i++) {
					if(data.containsKey(i)) {
						sendData+= i + " " + data.get(i) + " ";
						data.remove(i);
					}
				}
				dos.writeUTF(sendData);
				dataRange[0] = newDataRange[1] + 1;  

				printData();
				System.out.println("New predecessor is: " + predID);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static void printData() {
		for(int i = dataRange[0]; i <= dataRange[1] ; i++) {
			if(data.containsKey(i)) { 
				System.out.println("- " + i + " " + data.get(i)); 
			}
		}
	}
}