import java.io.PrintWriter;
import java.net.Socket;


public class beatsender implements Runnable{
	Socket socket;
	public beatsender(Socket socket){
		this.socket = socket;
	}
	public void run(){
		while(true){
			try{
			Thread.sleep(1000);
	        PrintWriter out = new PrintWriter(socket.getOutputStream(),true);
	        out.println("heartbeat");
			}catch(Exception e){
				
			}
		}
	}

}
