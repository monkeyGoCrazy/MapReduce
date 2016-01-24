import java.io.FileWriter;
import java.net.Socket;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;


public class finalReducer implements Runnable {
	public sharedata shared = null;
	public finalReducer(sharedata shared){
		this.shared = shared;
	}
	public void run(){
		String path;
    	while(!shared.finish()){
    		if((path=shared.getReduced()) != null){
    	//		System.out.println("Reduce");
    			shared.wordcount(path);
    	//		System.out.println(path);
    			shared.addDone(path);
    		}
    	}
    	String output = "output";
    	try{
        FileWriter fileWriter=new FileWriter(output, true);
    	Map<String,Integer> wordcount = shared.getfinal();
        Iterator iter = wordcount.entrySet().iterator();
        while (iter.hasNext()) {
        	Map.Entry entry = (Map.Entry) iter.next();
        	String key = (String) entry.getKey();
        	Integer val = (Integer) entry.getValue();
    		System.out.println(key+" "+val);
    		fileWriter.append(key+" "+val+'\n');
    	}
        fileWriter.close();
    	}catch(Exception e){   		
    	}
    	System.out.println("opps,total time is:");
    	long endTime=System.currentTimeMillis();
    	shared.setEndTime(endTime);
    	long time = shared.getRunTime();
    	System.out.println(time);
    	shared.setTer_signal();
    	try{
    	Thread.sleep(1000);
    	}catch(Exception e){}
    	System.exit(0);
	}

}
