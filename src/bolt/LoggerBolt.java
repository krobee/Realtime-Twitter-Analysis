package bolt;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import util.LossyEntry;
import util.MapUtil;


public class LoggerBolt implements IRichBolt{

	private Map<String, LossyEntry> map;
	private OutputCollector collector;
	private int interval;
	private BufferedWriter bw;
	private String logDir;

	public LoggerBolt(int interval, String logDir){
		this.interval = interval;
		this.logDir = logDir;
	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			bw = new BufferedWriter(new FileWriter(logDir, true));
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.map = new HashMap<String, LossyEntry>();
	}

	@Override
	public void execute(Tuple tuple) {
		if(isTickTuple(tuple) && map.size() != 0){
			try{

				String timestamp = new Timestamp(new java.util.Date().getTime()).toString();
				bw.write("<" + timestamp + ">");

				Map<String, LossyEntry> result = MapUtil.sortByValue(map);
				int count = 0;

				for(Map.Entry<String, LossyEntry> entry: result.entrySet()){
					if(count < 100){
						bw.write(entry.getValue().toString());
						count++;
					}	
				}

				bw.write("\n");
				bw.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		else{
			try{
				LossyEntry entry = (LossyEntry) tuple.getValueByField("entry");
				map.put(entry.getElement(), entry);
			} catch(Exception e){
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	}

	@Override
	public void cleanup() {
		try {
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, interval);
		return conf;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}
	
	public static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
}
