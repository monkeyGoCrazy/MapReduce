import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;


public class master implements Runnable{
	private sharedata shared;
	public cassandraEX database;
	public master(String filepath,String ip){
		try{
		database = new cassandraEX(ip,9160);
        database.open();  
        database.setKeySpace("wc"); 
        database.createColumnFamily("wc", "MapReduce");
		shared = new sharedata(filepath,database);
		}catch(Exception e){
			
		}
	}
	public void run(){
		try{
		String ip = "10.244.35.107";
        ServerSocket serverSocket = new ServerSocket(1234,3,InetAddress.getByName (ip));
        System.out.println("server created");
        long startTime=System.currentTimeMillis();
        shared.setStartTime(startTime);
        int client_num = 0;
        Thread finalReducer = new Thread(new finalReducer(shared));
        finalReducer.start();
        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("accept:"+clientSocket);
            client_num = client_num + 1;
            shared.addStatus(client_num, "true");
            Thread heartbeat = new Thread(new heartbeat(client_num,shared));
            heartbeat.start();
            shared.addSlave(client_num);
            Thread slaveControl = new Thread(new slaveControl(clientSocket,shared,client_num));
            slaveControl.start();
            Thread termination = new Thread(new termination(shared,clientSocket));
            termination.start();
     

        }
		}catch (IOException e){			
		}catch(Exception e){
			
		}
	}
}
