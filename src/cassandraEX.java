import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
/**
 * java  cassandra
 * @author menglei
 */
public class cassandraEX {
	private TTransport tr;
	private Cassandra.Client client;

	public cassandraEX(String ip, int port) {
		this.tr = new TFramedTransport(new TSocket(ip, port));// 9160
		TProtocol proto = new TBinaryProtocol(tr);
		this.client = new Cassandra.Client(proto);
	}

	public void open() throws Exception {
		tr.open();
		if (!tr.isOpen()) {
			throw new Exception("connect failed");
		}
	}

	public void close() {
		tr.close();
	}

	public void setKeySpace(String keyspace) throws Exception {
		client.set_keyspace(keyspace);// 使用myKeyspace keyspace
	}
	public void createKeySpace(String keyspace) throws Exception{
		
	}
	public void createColumnFamily(String keyspace,String columnfamily) throws Exception{
		CfDef cfDef = new CfDef();
		cfDef.keyspace = keyspace;
		cfDef.name = columnfamily;
		client.system_add_column_family(cfDef);
	}

	/**
	 * insert
	 * @throws Exception
	 */
	public void insert(String columnFamily,String key,String column,String value) throws Exception {
		ColumnParent parent = new ColumnParent(columnFamily);// column family
		long timestamp = System.currentTimeMillis();// 时间戳
		Column nameColumn = new Column(toByteBuffer(column));
		nameColumn.setValue(toByteBuffer(value));
		nameColumn.setTimestamp(timestamp);
		ByteBuffer nameColumnKey = toByteBuffer(key);
		client.insert(nameColumnKey, parent, nameColumn,ConsistencyLevel.ONE);
	}
	/**
	 * search one column
	 * @param key
	 * @param columnName
	 * @param columnFamily
	 * @throws Exception
	 */
	public void findOneColumn(String key, String columnName, String columnFamily) throws Exception {
		ColumnPath path = new ColumnPath(columnFamily); 
		path.setColumn(toByteBuffer(columnName)); // 读取id

		ColumnOrSuperColumn column = client.get(toByteBuffer(key), path, ConsistencyLevel.ONE);
		System.out.println(toString(column.column.name) + "->" + toString(column.column.value));
	}

	/**
	 * search all column of one columnfamily
	 * @param key
	 * @param columnFamily
	 * @throws Exception
	 */
	public void findAllColumn(String key, String columnFamily) throws Exception {
		ColumnParent parent = new ColumnParent(columnFamily);// column family

		SlicePredicate predicate = new SlicePredicate();
		SliceRange sliceRange = new SliceRange(toByteBuffer(""),
				toByteBuffer(""), false, 10);
		predicate.setSlice_range(sliceRange);
		List<ColumnOrSuperColumn> results = client.get_slice(toByteBuffer(key),
				parent, predicate, ConsistencyLevel.ONE);

		for (ColumnOrSuperColumn result : results) {
			System.out.print("{" + toString(result.column.name) + " -> "
					+ toString(result.column.value) + "}  ");
		}
		System.out.println();
	}



	public Map<String,Integer> wordcount(String path)throws Exception{
		Map<String,Integer> hash= new HashMap<String,Integer>();
		ColumnParent parent = new ColumnParent("MapReduce");// column family
	    SlicePredicate predicate = new SlicePredicate();
	    SliceRange sliceRange = new SliceRange(toByteBuffer(""), toByteBuffer(""), false, 300000);
	    predicate.setSlice_range(sliceRange);
	   
	    List<ColumnOrSuperColumn> results = client.get_slice(toByteBuffer(path), parent, predicate, ConsistencyLevel.ONE);
	    for (ColumnOrSuperColumn result : results)
	    {
	    Column column = result.column;
	    String key1 = toString(column.name);
	    String value1 = toString(column.value);
	  //  System.out.println(key1+value1);
		if(hash.containsKey(key1)){
			int i = hash.get(key1);
			i = i + Integer.valueOf(value1);
			hash.put(key1, i);
		}else{
		//    System.out.println(key1);
		    hash.put(key1, Integer.valueOf(value1));
		}
	    }
		return hash;
	}
	public Map<String,Integer> wordreduce(String path,Map<String,Integer> hash)throws Exception{
		ColumnParent parent = new ColumnParent("MapReduce");// column family
	    SlicePredicate predicate = new SlicePredicate();
	    SliceRange sliceRange = new SliceRange(toByteBuffer(""), toByteBuffer(""), false, 300000);
	    predicate.setSlice_range(sliceRange);
	   
	    List<ColumnOrSuperColumn> results = client.get_slice(toByteBuffer(path), parent, predicate, ConsistencyLevel.ONE);
	    for (ColumnOrSuperColumn result : results)
	    {
	    Column column = result.column;
	    String key1 = toString(column.name);
	    String value1 = toString(column.value);
		if(hash.containsKey(key1)){
			int i = hash.get(key1);
			i = i + Integer.valueOf(value1);
			hash.put(key1, i);
		}else{
		    hash.put(value1, Integer.valueOf(value1));
		}
	    }
		return hash;
	}
	
	/**
	 * delete
	 * @param key
	 * @param columnFamily
	 * @throws Exception
	 */
	public void remove(String key,String columnFamily)throws Exception{
		ColumnPath path = new ColumnPath(columnFamily); 
		long temp = System.currentTimeMillis();
		client.remove(toByteBuffer(key), path, temp, ConsistencyLevel.ONE);
	}
	

		
	public static ByteBuffer toByteBuffer(String value) throws Exception {
		return ByteBuffer.wrap(value.getBytes("UTF-8"));
	}

	public static String toString(ByteBuffer buffer) throws Exception {
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes, "UTF-8");
	}
}
