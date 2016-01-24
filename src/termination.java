import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;


public class termination implements Runnable {
	sharedata shared;
	Socket socket;
	public termination (sharedata shared, Socket socket){
		this.shared = shared;
		this.socket = socket;
	}
	public void run(){
		try {
			PrintWriter out = new PrintWriter(socket.getOutputStream(),true);
			while(shared.getTer_signal().equals("FINISH")){
				out.println("FINISH");
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
