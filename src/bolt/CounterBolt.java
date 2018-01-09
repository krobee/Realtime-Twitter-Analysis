package bolt;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import util.LossyEntry;
import util.MapUtil;

public class CounterBolt implements IRichBolt {

	private int width;
	private int interval; 
	private double epsilon, threshold;
	private int b_current = 1;
	private int N = 0;

	private Map<String, LossyEntry> map;
	private OutputCollector collector;

	public CounterBolt(int width, double threshold, int interval){
		this.width = width;
		this.interval = interval;
		this.epsilon = 1/(double)width;
		this.threshold = threshold;
	}


	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.map = new HashMap<String, LossyEntry>();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {

		if(isTickTuple(tuple) && map.size() != 0){
			Map<String, LossyEntry> result = MapUtil.sortByValue(map);
			int count = 0;

			for(Map.Entry<String, LossyEntry> entry: result.entrySet()){
				if(count < 100){
					collector.emit(new Values(entry.getValue()));
					count++;
				}	
			}
		}
		else{
			try{
				LossyEntry entry = (LossyEntry) tuple.getValueByField("entry");
				lossyCounting(entry);
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

	public void lossyCounting(LossyEntry entry){
		String key = entry.getElement();
		int svalue = entry.getSvalue();

		// insert phase
		if(!map.containsKey(key)){
			LossyEntry newEntry = new LossyEntry(key, 1, b_current-1, svalue);
			map.put(key, newEntry);
		}
		else{
			LossyEntry oldEntry = map.get(key);
			oldEntry.setFrequency(oldEntry.getFrequency()+1);
			oldEntry.setSvalue(oldEntry.getSvalue() + svalue);
			map.put(key, oldEntry);
		}
		N++;

		// delete phase
		if(N % width == 0){
			Iterator it = map.entrySet().iterator();
			while (it.hasNext())
			{
				Map.Entry<String, LossyEntry> temp = (Entry) it.next();
				LossyEntry e = temp.getValue();
				if(e.getFrequency() + e.getDelta() <= b_current){
					it.remove();
				}
				else if(e.getFrequency() < ((threshold-epsilon)*N)){
					it.remove();
				}
			}
			b_current++;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
				declarer.declare(new Fields("entry"));
	}

	public static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, interval);
		return conf;
	}


	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
