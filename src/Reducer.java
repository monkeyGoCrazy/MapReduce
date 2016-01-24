import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

public class Reducer implements Runnable {
	public cassandraEX database = null;
	public Socket socket;
	public String cpath;
	public Reducer(String cpath,cassandraEX c,Socket socket){
		this.database = c;
		this.socket = socket;
		this.cpath = cpath;
	}
	public void run(){
    	try{
    	Map<String,Integer>  hash = new HashMap<String,Integer>();
    	database.wordreduce(cpath, hash);
    	String rpath = "reduce"+cpath;
        Iterator iter = hash.entrySet().iterator();
        while (iter.hasNext()) {
        	System.out.println("hash");
        	Map.Entry entry = (Map.Entry) iter.next();
        	String key = (String) entry.getKey();
        	String val = (String) entry.getValue();
        	database.insert("MapReduce", rpath, key, val);
        }
        PrintWriter out = new PrintWriter(socket.getOutputStream(),true);
        out.println("REDUCEDONE"+" "+rpath);
    	}catch(Exception e){
    		
    	}
	}
}
