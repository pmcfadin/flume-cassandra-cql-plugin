package com.hobsons.flume.plugin;

import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.safehaus.uuid.UUID;
import org.safehaus.uuid.UUIDGenerator;



import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;


import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class CassandraCqlSink extends EventSink.Base{
	  
    private static final String KS_CDRLOG = "CDRLogs";
    private static final String CLUSTER_NAME = "Test Cluster";
    private static final String CF_ENTRY = "CDREntry";
    private static final String CF_MSISDN = "MSISDNTimeline";
    private static final String CF_HOURLY = "HourlyTimeline";
    private static final StringSerializer stringSerializer = StringSerializer.get();
    private static final BytesArraySerializer bytesSerializer = BytesArraySerializer.get();


    private Cluster cluster;
    private Keyspace keyspace;
    private Mutator<byte[]> mutator;
    private String m_CFRawCdr;

    private static final UUIDGenerator uuidGen = UUIDGenerator.getInstance();

    private static final String entryColumnFamily = "CDREntry";
    private static final String msisdnColumnFamily = "MSISDNTimeLine";
    private static final String hourlyColumnFamily = "HourlyTimeLine";
    private static final String rawColumnFamily = "RawCDREntry";

    private static final long MILLI_TO_MICRO = 1000; // 1ms = 1000us

    private static final String[] CDRENTRY_NAME = {"type",
						   "market",
						   "id",
						   "timestamp",
						   "moipaddress",
						   "mtipaddress",
						   "msisdn",
						   "senderdomain",
						   "recipientdomain"};
    private static final int[] CDRENTRY_MAP = {0,1,2,4,6,7,8,10,11};

    public CassandraCqlSink(String server, String cfRawData) {
    
	cluster = getOrCreateCluster(CLUSTER_NAME, server);
	keyspace = createKeyspace(KS_CDRLOG, cluster);
	mutator = createMutator(keyspace, bytesSerializer);

	//Add cdRawData.

	m_CFRawCdr = cfRawData;
	BasicColumnFamilyDefinition cfo = new BasicColumnFamilyDefinition();
	cfo.setColumnType(ColumnType.STANDARD);
	cfo.setName(cfRawData);
	cfo.setComparatorType(ComparatorType.BYTESTYPE);
	cfo.setKeyspaceName(KS_CDRLOG);

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
		long timestamp = System.currentTimeMillis() * MILLI_TO_MICRO;

		// Make the index column
		UUID uuid = uuidGen.generateTimeBasedUUID();

	    //CDREntry
	    //
	    //CDR format is
	    //
	    //op,market,tid,mdr_type,msg_ts,imsi,mo_ip,mt_ip,ptn,msg_type,mo_domain,mt_domain
		String rawEntry = new String(event.getBody());

		String[] rawEntries = rawEntry.split("\\,");
		for(int i = 0; i < CDRENTRY_NAME.length; i++) {
		    mutator.addInsertion(uuid.toString().getBytes(),
				  CF_ENTRY,
				  createColumn(CDRENTRY_NAME[i].getBytes(),
				    rawEntries[CDRENTRY_MAP[i]].getBytes()));
		}

		//MSISDNTimeLine & HourlyTimeLine
		String msisdn = new String(rawEntries[8]);
		mutator.addInsertion(msisdn.getBytes(),
			      CF_MSISDN,
			      createColumn(Long.toString(timestamp).getBytes(),
					   uuid.toByteArray()));
 
		mutator.addInsertion(Long.toString(timestamp).getBytes(),
			      CF_HOURLY,
			      createColumn(Long.toString(timestamp).getBytes(),
					   uuid.toByteArray()));
 
		mutator.addInsertion(uuid.toString().getBytes(),
			      m_CFRawCdr,
			      createColumn(uuid.toByteArray(),
					   event.getBody()));


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
              "usage: CDRCassandraSink(\"host:port\", \"raw_cdr_column_family\")");
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
	builders.add(new Pair<String, SinkBuilder>("CDRCassandraSink", builder()));
	return builders;
    }

    private HColumn<byte[], byte[]> createColumn(byte[] name, byte[] value) {
	return HFactory.createColumn(name, value, bytesSerializer, bytesSerializer);
    }

}

