package topology;
import java.io.File;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import bolt.CounterBolt;
import bolt.LoggerBolt;
import bolt.ReaderBolt;
import spout.TwitterSpout;


public class TopologyParallel {

	private static final int TIME_INTERVAL = 10;
	private static final int BUCKET_WIDTH = 200;
	
	// threshold > 1/width
	private static final double THRESHOLD = 0.01;
	
	private static final String HASHTAG_LOG = "/s/bach/d/under/ravno/cs535P2/P2/hashtag-logP";
	private static final String NAMEDENT_LOG = "/s/bach/d/under/ravno/cs535P2/P2/namedent-logP";

	public static void main(String[] args){
		// delete existed files
		File file = new File(HASHTAG_LOG);
		if(file.exists())	
			file.delete();
		file = new File(NAMEDENT_LOG);
		if(file.exists())	
			file.delete();

		// topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new TwitterSpout());
		
		// reader bolt
		builder.setBolt("reader-bolt", new ReaderBolt() ,1).shuffleGrouping("spout");
				
		// counter bolt
		builder.setBolt("hashtag-counter-bolt", new CounterBolt(BUCKET_WIDTH, THRESHOLD, TIME_INTERVAL), 4).fieldsGrouping("reader-bolt", "hashtag-stream", new Fields("entry"));
		builder.setBolt("namedent-counter-bolt", new CounterBolt(BUCKET_WIDTH, THRESHOLD, TIME_INTERVAL), 4).fieldsGrouping("reader-bolt", "namedent-stream", new Fields("entry"));
		
		// logger bolt
		builder.setBolt("hashtag-logger-bolt", new LoggerBolt(TIME_INTERVAL, HASHTAG_LOG), 1).globalGrouping("hashtag-counter-bolt");
		builder.setBolt("namedent-logger-bolt", new LoggerBolt(TIME_INTERVAL, NAMEDENT_LOG), 1).globalGrouping("namedent-counter-bolt");
		
		
		// config
		Config config = new Config();
		config.setDebug(false);
//		config.setTopologyWorkerMaxHeapSize(4096.0);
		config.setNumWorkers(4);

		// submit topology
		try {
			StormSubmitter.submitTopology("twitterP", config, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		} catch (AuthorizationException e) {
			e.printStackTrace();
		}
	}
}
