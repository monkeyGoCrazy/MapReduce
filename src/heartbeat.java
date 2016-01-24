
public class heartbeat implements Runnable{
	int client_num;
	sharedata shared;
	public heartbeat(int client_num,sharedata shared){
		this.client_num=client_num;
		this.shared =shared;
	}
	public void run(){
		while(shared.getStatus(client_num)){
			try{
				Thread.sleep(1000);
				int i = shared.getTime(client_num);
				i--;
				shared.addTime(client_num, i);
				if(i==0){
					shared.addStatus(client_num, "false");
					
				}
			}catch(Exception e){
				
			}
		}
	}
}
