import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;


public class slave implements Runnable{
	public String local_ip;
	public String server_ip;
	public slave(String local_ip,String server_ip){
		this.local_ip = local_ip;
		this.server_ip = server_ip;
	}
	public void run(){
		Integer port = 9160;
		PrintWriter out;
		BufferedReader in;
		String fromServer;
		cassandraEX database = new cassandraEX(local_ip, port);

		try{
	        database.open();  
	        database.setKeySpace("wc"); 
			Socket socket = new Socket(server_ip,1234);
	        out = new PrintWriter(socket.getOutputStream(),true);
	        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
  	        beatsender s = new beatsender(socket);
	        Thread b = new Thread(s);
	        b.start();	        
	        while ((fromServer = in.readLine()) != null){
                String message[]=fromServer.split("\\s+");
                if (message[0].equals("MAP")){
        	         mapper m = new mapper(message[1],message[2],database,socket);
        	         Thread t = new Thread(m);
        	         t.start();
                }
                if (message[0].equals("REDUCE")){
       	         Reducer m = new Reducer(message[1],database,socket);
       	         Thread t = new Thread(m);
       	         t.start();
               }
                if (message[0].equals("FINISH")){
                	System.exit(0);
                }
	        }
        }catch (UnknownHostException e) {
            System.err.println("Don't know about host"+local_ip);
            System.exit(1);
        }catch (IOException e) {
            System.err.println("Couldn't get I/O");
           
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
