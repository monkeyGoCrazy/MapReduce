public class MapReduce {
	public MapReduce(){
	}
	public static void main(String args[]){
		if(args[0].equals("master")){
			Thread m = new Thread(new master(args[1],args[2]));
			
			m.start();
		}
		if(args[0].equals("slave")){
			Thread m = new Thread(new slave(args[1],args[2]));
			m.start();                
		}
	}
}
