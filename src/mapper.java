import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

public class mapper implements Runnable {
	public String path;
	public cassandraEX database = null;
	public Socket socket;
	public String cpath;
	public mapper(String filepath,String cpath,cassandraEX c,Socket socket){
		this.path = filepath;
		this.database = c;
		this.socket = socket;
		this.cpath = cpath;
	}
	public void run(){
    	String word;
    	try{
    	Map<String,Integer>  hash = new HashMap<String,Integer>();
        BufferedReader input = new BufferedReader(new FileReader(path));
        while ((word = input.readLine()) != null){
			if(hash.containsKey(word)){
				int i = hash.get(word);
				i++;
				hash.put(word, i);
			}else{
				hash.put(word, 1);
			}
        }
        Iterator iter = hash.entrySet().iterator();
        while (iter.hasNext()) {
        	Map.Entry entry = (Map.Entry) iter.next();
        	String key = (String) entry.getKey();
        	key = key+" ";
        	Integer val = (Integer) entry.getValue();
        	String value = val+"";
        	database.insert("MapReduce", cpath, key, value);
        }
        PrintWriter out = new PrintWriter(socket.getOutputStream(),true);
        out.println("MAPDONE"+" "+cpath);
    	}catch(Exception e){
    		
    	}
	}
}
