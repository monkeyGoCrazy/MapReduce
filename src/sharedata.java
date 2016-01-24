import java.io.File;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

//help master deal with status of the slave and task
public class sharedata {
	private LinkedList<String> toBeMapped= new LinkedList<String>();
	private LinkedList<String> totalFile = new LinkedList<String>();
	private LinkedList<String> toBeReduced = new LinkedList<String>();
	private LinkedList<String> Reduced = new LinkedList<String>();
	private LinkedList<String> Done = new LinkedList<String>();
	private LinkedList<Integer> Slave = new LinkedList<Integer>();
	private Map<String,Integer> wordcount= new HashMap<String,Integer>();
	private Map<Integer,String> status = new HashMap<Integer,String>();
	private Map<Integer,Integer> time = new HashMap<Integer,Integer>();
	private String ter_signal = "UNFINISH";
	private long startTime;
	private long endTime;
	private long runTime;
	public cassandraEX database = null;
	public sharedata(String filepath,cassandraEX database){
		this.database=database;
		File file = new File(filepath);
        String[] filelist = file.list();
        for (int i = 0; i < filelist.length; i++) {
        	toBeMapped.add(filepath + "/" + filelist[i]+" "+i);
        	totalFile.add(filepath + "/" + filelist[i]+" "+i);
        	
        }
	}
	public synchronized void setTer_signal(){
		this.ter_signal = "FINISH";
	}
	public synchronized String getTer_signal(){
		return this.ter_signal;
	}
	public synchronized void setStartTime(long startTime){
		this.startTime = startTime;
	}
	public synchronized void setEndTime(long endTime){
		this.endTime = endTime;
	}
	public synchronized long getRunTime(){
		this.runTime = this.endTime-this.startTime;
		return this.runTime;
	}
	public synchronized void addStatus(Integer client_num,String s){
		status.put(client_num, s);
	}
	public synchronized void addTime(Integer client_num, Integer i){
		time.put(client_num, i);
	}
	public synchronized boolean getStatus(Integer client_num){
		String s = status.get(client_num);
		return Boolean.parseBoolean(s);
	}
	public synchronized Integer getTime(Integer client_num){
		return time.get(client_num);
	}
	
	public synchronized void addDone(String filepath){
		Done.add(filepath);
	}
	public synchronized boolean finish(){
		if(totalFile.size() == Done.size()){
			return true;
		}else{
			return false;
		}
	}
	public synchronized void addReduced(String filepath){
		Reduced.add(filepath);
	}
	public synchronized String getReduced(){
		if(Reduced.size()!=0){
			return Reduced.removeFirst();
		}else{
			return null;
		}
	}
	public synchronized void wordcount(String filepath){
		try {
			Map<String,Integer> wordcount2 = database.wordcount(filepath);
	        Iterator iter = wordcount2.entrySet().iterator();
	        while (iter.hasNext()) {
	        	Map.Entry entry = (Map.Entry) iter.next();
	        	String key = (String) entry.getKey();
	        	Integer val = (Integer) entry.getValue();
	    		if(wordcount.containsKey(key)){
	    			int i = wordcount.get(key);
	    			i = i + Integer.valueOf(val);
	    			wordcount.put(key, i);
	    		}else{
	    		//    System.out.println(key1);
	    		    wordcount.put(key, val);
	    		}
	        }
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public synchronized void remove(String filepath){
		try {
			database.remove(filepath, "MapReduce");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public synchronized Map<String,Integer> getfinal(){
		return wordcount;
	}
	public synchronized String getToBeMapped(){
		if(toBeMapped.size()!=0){
			return toBeMapped.removeFirst();
		}else{
			return null;
		}
	}
	public synchronized void addToBeMapped(String filepath){
		toBeMapped.add(filepath);
	}
	public synchronized void addSlave(Integer slave){
		Slave.add(slave);
	}
	public synchronized Integer getSlave(){
		if(Slave.size()!=0){
			return Slave.removeFirst();
		}else{
			return null;
		}
	}
	public synchronized String getToBeReduced(){
		if(toBeReduced.size()!=0){
			return toBeReduced.removeFirst();
		}else{
			return null;
		}
	}
	public synchronized void addToBeReduced(String filepath){
		toBeReduced.add(filepath);
	}
}
