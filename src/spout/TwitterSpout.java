package spout;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSpout implements IRichSpout {
	
	SpoutOutputCollector collector;
	LinkedBlockingQueue<Status> queue;
	TwitterStream twitterStream;

	private static final String consumerKey = "BgpIxeGXyyZttP5FkDHIcDFFK";
	private static final String consumerSecret = "ILFAE1BxwDNCw13Cqr8mxY6ayJvxr6vEez0oxY9IOpJSqKgQTN";
	private static final String accessToken = "917948245531295744-JnBJ590jhv9wZzXikkdyoQKRa92RNXV";
	private static final String accessTokenSecret = "B97yrMUBt0kvZ8usXnsnKrJdJHfFkpOjwbdw2XFzXQGxK";

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<>();
		this.collector = collector;
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				if(status.getLang().equals("en")){
					queue.offer(status);
				}
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {}

			@Override
			public void onTrackLimitationNotice(int i) {}

			@Override
			public void onScrubGeo(long l, long l1) {}

			@Override
			public void onException(Exception ex) {}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
			}
		};

		ConfigurationBuilder cb = new ConfigurationBuilder();

		cb.setDebugEnabled(true)
		.setOAuthConsumerKey(consumerKey)
		.setOAuthConsumerSecret(consumerSecret)
		.setOAuthAccessToken(accessToken)
		.setOAuthAccessTokenSecret(accessTokenSecret);

		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		twitterStream.addListener(listener);
		twitterStream.sample();
	}

	@Override
	public void nextTuple() {
		Status status = queue.poll();

		if (status == null) {
			Utils.sleep(1000);
		} else {
			collector.emit(new Values(status));
		}
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
