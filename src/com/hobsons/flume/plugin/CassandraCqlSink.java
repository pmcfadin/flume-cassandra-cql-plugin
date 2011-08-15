package com.hobsons.flume.plugin;

import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.safehaus.uuid.UUID;
import org.safehaus.uuid.UUIDGenerator;



import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;


import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
//import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
//import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class CassandraCqlSink extends EventSink.Base{
	
	private static final Properties prop = getProps();
    private static final String KS_LOG = (String)prop.getProperty("KS_LOG");
    private static final String CLUSTER_NAME = (String)prop.getProperty("CLUSTER_NAME");
    private static final String CF_ENTRY = (String)prop.getProperty("CF_ENTRY");
//    private static final String CF_HOURLY = (String)prop.getProperty("CF_HOURLY");
    private static final StringSerializer stringSerializer = StringSerializer.get();
    
//    private Long startTime = System.nanoTime();
    private Cluster cluster;
    private Keyspace keyspace;
    private Mutator<String> mutator;
//    private String CFRaw;
    private UUID minute;
    private Timer timer = new Timer();
    
    
    private static final UUIDGenerator uuidGen = UUIDGenerator.getInstance();

    public CassandraCqlSink(String server, String cfRawData) {
    	  
	cluster = getOrCreateCluster(CLUSTER_NAME, createConfig(server,true));
	keyspace = createKeyspace(KS_LOG, cluster);
	mutator = createMutator(keyspace, stringSerializer);

//	CFRaw = cfRawData;
	BasicColumnFamilyDefinition cfo = new BasicColumnFamilyDefinition();
	cfo.setColumnType(ColumnType.STANDARD);
	cfo.setName(cfRawData);
	cfo.setComparatorType(ComparatorType.BYTESTYPE);
	cfo.setKeyspaceName(KS_LOG);
	
	
	//Timer to generate new Time-based UUID for each minute
//	startTime=System.nanoTime();
    	timer.schedule(new TimerTask() {
            public void run() {
                minute = uuidGen.generateTimeBasedUUID();
            }
        } , 60000 , 60000);


	try {
	    cluster.addColumnFamily(new ThriftCfDef((cfo)));
	} catch (HInvalidRequestException e) {
	    //e.printStackTrace();
	    //Ignore for now. CF could already exist, which need not be
	    //an error.
	}
    }

    @Override
    public void open() throws IOException {
	//Do nothing
    }

    /**
     * Writes the message to Cassandra.
    * The key is the current date (YYYYMMDD) and the column
    * name is a type 1 UUID, which includes a time stamp
    * component.
    */
    @Override
    public void append(Event event) throws IOException, InterruptedException {

	if (event.getBody().length > 0) {
	    try {
	    	
	    String rawEntry = new String(event.getBody());	 
	    
//	    long elapsedTime = System.nanoTime() - startTime;
//	    double seconds = (double)elapsedTime / 1000000000.0;

	    
		// Make the index column
	    if(minute==null){
	    	minute = uuidGen.generateTimeBasedUUID();
	    }

		UUID uuid = uuidGen.generateTimeBasedUUID();
		mutator.addInsertion(minute.toString(),CF_ENTRY,HFactory.createStringColumn(uuid.toString(),rawEntry));
		
		
//		System.out.println("One Minute? "+ seconds);
//	    System.out.println(minute.toString());

		mutator.execute();

	    
	    
	    } catch (HInvalidRequestException e) {
		e.printStackTrace();
		throw new IOException("Failed to process log entry");
	    }
	}

	super.append(event);
    }

    @Override
    public void close() throws IOException {
	//Do nothing.
    }

    public static SinkBuilder builder() {
    	
	return new SinkBuilder() {
	@Override
	public EventSink build(Context context, String ... args) {
		
	    if (args.length < 2) {
          throw new IllegalArgumentException(
              "usage: cassandraCqlSink(\"host:port\", \"raw_cdr_column_family\")");
        }
	    
       return new CassandraCqlSink(args[0], args[1]);
      }
    };  
    }

  /**
   * This is a special function used by the SourceFactory to pull in this class
   * as a plugin sink.
   */
    public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
    	
		List<Pair<String, SinkBuilder>> builders =
		new ArrayList<Pair<String, SinkBuilder>>();
		builders.add(new Pair<String, SinkBuilder>("cassandraCqlSink", builder()));
		return builders;
    }
    
    private static CassandraHostConfigurator createConfig(String hosts, boolean autoDiscover){
    	
    	CassandraHostConfigurator hostConfig = new CassandraHostConfigurator(hosts);
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

}


