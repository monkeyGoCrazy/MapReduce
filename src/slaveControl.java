import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Hashtable;
import java.util.Iterator;


public class slaveControl implements Runnable {
	Socket socket;
	PrintWriter out;
	BufferedReader in;
	String fromServer;
	sharedata shared = null;
	String path;
	int client_num;
	public slaveControl(Socket socket,sharedata shared,int client_num){
		this.socket = socket;
		this.shared = shared;
		this.client_num =client_num;
	}
	public void run() {
		// TODO Auto-generated method stub
		try{
		String msg = "msg";
		String working = "null";
        out = new PrintWriter(socket.getOutputStream(),true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        if((path=shared.getToBeMapped()) !=null){
        	msg = "MAP"+" "+path;
        	System.out.println("Map task starts on "+client_num+" at "+path);
        	out.println(msg);
        }else if((path=shared.getToBeReduced()) !=null){
        	msg = "REDUCE"+" "+path;
        	System.out.println("Reduce task starts on"+path);
        	out.print(msg);
        }else{
        	out.println("FINISH");
        }
        while ((fromServer = in.readLine()) != null){
        	String message[]=fromServer.split("\\s+");
        	if(message[0].equals("MAPDONE")){
        		
        		shared.addReduced(message[1]);
                if((path=shared.getToBeMapped()) !=null){
                	msg = "MAP"+" "+path;
                	System.out.println("Map task starts on "+client_num+" at "+path);
                	out.println(msg);
                }else if((path=shared.getToBeReduced()) !=null){
                	msg = "REDUCE"+" "+path;
                	System.out.println("Reduce task starts on "+client_num+" at "+path);
                	out.println(msg);
                }
        	}
        	if(message[0].equals("REDUCEDONE")){
        		shared.addReduced(message[1]);
                if((path=shared.getToBeReduced()) !=null){
                	msg = "REDUCE"+" "+path;
                	System.out.println("Reduce task starts on "+client_num+" at "+path);
                	out.println(msg);
                }

        	}
        	if(message[0].equals("heartbeat")){
        		shared.addTime(client_num,3);
        	}
        }

	//	}
		String message[] =msg.split("\\s+");
		if(message[0].equals("MAP")){
			System.out.println(msg);
			shared.remove(message[1]);
			shared.addToBeMapped(message[1]+" "+message[2]);
		}else{
			shared.remove(message[1]);
			shared.addToBeReduced(message[1]);
		}
		}catch (IOException e){
			
		}
	}

}
