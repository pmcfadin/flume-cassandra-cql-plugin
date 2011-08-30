package com.hobsons.flume.plugin;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;

//
import java.text.SimpleDateFormat;

import org.safehaus.uuid.UUIDGenerator;
//
import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;

import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class CassandraCqlSink extends EventSink.Base {

	private static final Properties prop = getProps();
	private static final String KEYSPACE = (String) prop
			.getProperty("KEYSPACE");
	private static final String CLUSTER_NAME = (String) prop
			.getProperty("CLUSTER_NAME");
	private static final String CF_RAW = (String) prop.getProperty("CF_RAW");
	private static final StringSerializer se = StringSerializer.get();

	private static final int PRODUCTCODE = 0;
	private static final int DATACENTER = 1;
//	private static final int SESSIONID = 2;
	private static final int TIMESTAMP = 3;
//	private static final int CLIENTIP = 4;
//	private static final int INTERNALIP = 5;
//	private static final int HTTPMETHOD = 6;
	private static final int URL = 7;
//	private static final int HTTPQUERY = 8;
//	private static final int HTTPREF = 9;
	private static final int CUSTOMER = 10;
//	private static final int SERVERADDR = 11;
	private static final int RESPTIME = 12;
//	private static final int HTTPSTATUS = 13;

	private Cluster cluster;
	private Keyspace ko;
	private Mutator<String> mutator;
	private static Calendar cal = Calendar.getInstance();
	private List<ColumnFamilyDefinition> cfsList = new ArrayList<ColumnFamilyDefinition>();
	private List<String> cfs = new ArrayList<String>();
	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

	public CassandraCqlSink(String server, String cfRawData, String autoDiscover) {

	 System.out.println("Flume-Cassandra plugin initialized");
		
		cluster = getOrCreateCluster(CLUSTER_NAME,
				createConfig(server, Boolean.parseBoolean(autoDiscover)));

		if (cluster.describeKeyspace("syslog_web_perf") == null) {

			BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
			columnFamilyDefinition.setKeyspaceName(KEYSPACE);
			columnFamilyDefinition.setName(CF_RAW);
			columnFamilyDefinition.setComparatorType(ComparatorType.ASCIITYPE);

			ColumnFamilyDefinition cfdef = new ThriftCfDef(
					columnFamilyDefinition);

			KeyspaceDefinition ksdef = HFactory.createKeyspaceDefinition(
					KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", 3,
					Arrays.asList(cfdef));

			cfs.add(cfdef.getName());
			cluster.addKeyspace(ksdef);
			
	    	System.out.println("Initializing Keyspace and Raw Column Family...");
	    	try {
	    		Thread.sleep(1000);
	    		} catch(InterruptedException e) {
	    		} 
	    	System.out.println("Done");
	
//TODO			//Needs to wait to see if the CF added properly
		}

		ko = createKeyspace(KEYSPACE, cluster);
		mutator = createMutator(ko, se);
		
		cfsList = cluster.describeKeyspace(KEYSPACE).getCfDefs();
		Iterator<ColumnFamilyDefinition> cfsItr = cfsList.iterator();
		
		while (cfsItr.hasNext()) {

			cfs.add(cfsItr.next().getName());
		}

	}

	@Override
	public void open() throws IOException {
		// Do nothing
	}

	/**
	 * Writes the syslog data to Cassandra.
	 * @throws InterruptedException 
	 */
	@Override
    public void append(Event event) throws IOException, InterruptedException {

	if (event.getBody().length > 0) {
	    try {

	    String rawEntry = new String(event.getBody());	 
	    
	    rawEntry.replaceAll("x09", "\t");	 
	    String[] entry = rawEntry.split("\t");
	    String[] date = entry[TIMESTAMP].split(" ");
	    String[] hms  = date[1].split(":");
	    String[] dmy  = date[0].split("/");
	    String[] product = entry[PRODUCTCODE].split("-");
	    	    
//TODO	//added for entry that is too short and for images.
	    if(entry.length-1<13||entry[URL].contains(".png")||entry[URL].contains(".jpg")||entry[URL].contains(".gif"))
	    	return;
	    
	    StringBuffer cfName = (new StringBuffer(product[1])).append("_web_perf_data");
	    StringBuffer rawKey = new StringBuffer(dmy[2]);
	    	rawKey.append(dmy[0]);
	    	rawKey.append(dmy[1]);
	    	rawKey.append(hms[0]);
	    
		StringBuffer rawUUID = new StringBuffer(dmy[2]);
			rawUUID.append(dmy[0]);
			rawUUID.append(dmy[1]);
			rawUUID.append(hms[0]).append(hms[1]).append(hms[2]).append("-");
			rawUUID.append(UUID.randomUUID().toString());
	    	  
	    //Checks to see if the Raw Bucket CF and <product>_web_perf_data CF exist 
	    if(!cfs.contains(cfName.toString())){
	    	
	    	ColumnFamilyDefinition cfdef = HFactory.createColumnFamilyDefinition(KEYSPACE, cfName.toString(), ComparatorType.ASCIITYPE);
	    	cfdef.setDefaultValidationClass("CounterColumnType");
	    	
//	   		cfsList.add(cfdef);
	    	cfs.add(cfName.toString());
	    	cluster.addColumnFamily(cfdef);	
	    	
	    	System.out.println("Creating Columnfamily: "+cfdef.getName());
	    	try {
	    		Thread.sleep(1000);
	    		} catch(InterruptedException e) {
	    		} 
	    	System.out.println("Done");
	    	//Needs to wait to see if the CF added properly
	    }
	    
	    //Raw Insertion - Hourly Buckets
	    mutator.insert(rawKey.toString(), CF_RAW, HFactory.createStringColumn(rawUUID.toString(), rawEntry));
	    		    
	    //Insertions for each counter (16 Total)	    
	    //dc;url;customer;year
	    StringBuffer rowKey = new StringBuffer(entry[DATACENTER]);	 
	    rowKey.append(";");
	    rowKey.append(entry[URL]);
	    rowKey.append(";");
	    rowKey.append(entry[CUSTOMER]);
	    rowKey.append(";");
	    rowKey.append(dmy[2]);
	    
//debug		System.out.println(rawEntry);
	    
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_resp_time")).toString()
	    				, Long.parseLong(entry[RESPTIME])));    
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_count")).toString()
	    				, 1));
	    
	    //dc;url;year
	    rowKey = new StringBuffer(entry[DATACENTER]);	
	    rowKey.append(";");
	    rowKey.append(entry[URL]);
	    rowKey.append(";");
	    rowKey.append(dmy[2]);  
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_resp_time")).toString()
	    				, Long.parseLong(entry[RESPTIME])));    
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_count")).toString()
	    				, 1));
	    
	    //dc;customer;year
	    rowKey = new StringBuffer(entry[DATACENTER]);	
	    rowKey.append(";");
	    rowKey.append(entry[CUSTOMER]);
	    rowKey.append(";");
	    rowKey.append(dmy[2]);  
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_resp_time")).toString()
	    				, Long.parseLong(entry[RESPTIME])));    
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_count")).toString()
	    				, 1));
	    
	    //dc;year
	    rowKey = new StringBuffer(entry[DATACENTER]);	
	    rowKey.append(";");
	    rowKey.append(dmy[2]);  
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_resp_time")).toString()
	    				, Long.parseLong(entry[RESPTIME])));    
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_count")).toString()
	    				, 1));
	    
	    //url;customer;year
	    rowKey = new StringBuffer(entry[URL]);
	    rowKey.append(";");
	    rowKey.append(entry[CUSTOMER]);
	    rowKey.append(";");
	    rowKey.append(dmy[2]);  
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_resp_time")).toString()
	    				, Long.parseLong(entry[RESPTIME])));    
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_count")).toString()
	    				, 1));
	    
	    //url;year
	    rowKey = new StringBuffer(entry[URL]);
	    rowKey.append(";");
	    rowKey.append(dmy[2]);  
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_resp_time")).toString()
	    				, Long.parseLong(entry[RESPTIME])));    
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_count")).toString()
	    				, 1));
	    
	    //customer;year
	    rowKey = new StringBuffer(entry[CUSTOMER]);
	    rowKey.append(";");
	    rowKey.append(dmy[2]);  
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_resp_time")).toString()
	    				, Long.parseLong(entry[RESPTIME])));    
	    mutator.insertCounter(rowKey.toString(), cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_count")).toString()
	    				, 1));
	    
	    //year
	    mutator.insertCounter(dmy[2], cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_resp_time")).toString()
	    				, Long.parseLong(entry[RESPTIME])));    
	    mutator.insertCounter(dmy[2], cfName.toString(), 
	    		HFactory.createCounterColumn(((new StringBuffer(formatter.format(cal.getTime()))).append("_count")).toString()
	    				, 1)); 

	    } catch (HInvalidRequestException e) {
		e.printStackTrace();

		throw new IOException("Failed to process log entry");
	    }
	}

	super.append(event);
    }

	@Override
	public void close() throws IOException {
		// Do nothing.
	}

	public static SinkBuilder builder() {

		return new SinkBuilder() {
			@Override
			public EventSink build(Context context, String... args) {

				if (args.length < 2) {
					throw new IllegalArgumentException(
							"usage: cassandraCqlSink(\"host:port\", \"raw_data_column_family\")");
				}

				return new CassandraCqlSink(args[0], args[1], args[2]);
			}
		};
	}

	/**
	 * This is a special function used by the SourceFactory to pull in this
	 * class as a plugin sink.
	 */
	public static List<Pair<String, SinkBuilder>> getSinkBuilders() {

		List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
		builders.add(new Pair<String, SinkBuilder>("cassandraCqlSink",
				builder()));
		return builders;
	}

	private static CassandraHostConfigurator createConfig(String hosts,
			boolean autoDiscover) {

		CassandraHostConfigurator hostConfig = new CassandraHostConfigurator(
				hosts);
		hostConfig.setAutoDiscoverHosts(autoDiscover);

		return hostConfig;
	}

	static Properties getProps() {
		Properties properties = new Properties();

		try {
			properties.load(new FileInputStream("cassandra.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		return properties;
	}

	
	//TODO Set Time
	static UUID timeUUIDGen(String[] date, String[] hms) {
		
		cal.set(Integer.parseInt(date[2]), Integer.parseInt(date[1]) - 1,
				Integer.parseInt(date[0]),Integer.parseInt(hms[0]),Integer.parseInt(hms[1]),Integer.parseInt(hms[2]));

		final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
		long origTime = cal.getTime().getTime();
		long time = origTime * 10000 + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;
		long timeLow = time & 0xffffffffL;
		long timeMid = time & 0xffff00000000L;
		long timeHi = time & 0xfff000000000000L;
		long upperLong = (timeLow << 32) | (timeMid >> 16) | (1 << 12)
				| (timeHi >> 48);

		return new UUID(upperLong, 0xC000000000000000L);
	}
}
