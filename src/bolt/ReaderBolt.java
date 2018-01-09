package bolt;


import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.Triple;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import util.LossyEntry;

public class ReaderBolt implements IRichBolt {

	private OutputCollector collector;
	AbstractSequenceClassifier<CoreLabel> classifier = null;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;this.collector = collector;

		// prepare for named entity recognition
		String serializedClassifier = "edu/stanford/nlp/models/ner/english.muc.7class.distsim.crf.ser.gz";
		try {
			classifier = CRFClassifier.getClassifier(serializedClassifier);
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void execute(Tuple tuple) {

		// get value from spout
		Status tweet = (Status) tuple.getValueByField("tweet");

		// setup NLP
		Properties properties = new Properties();
		properties.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(properties);
		Annotation document = new Annotation(tweet.getText());
		pipeline.annotate(document);
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);

		// calculate cumulative svalues
		int svalue = 0;	
		for (CoreMap sentence : sentences) {
			String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
			if(sentiment.equalsIgnoreCase("neutral")) 
				svalue += 0;
			else if(sentiment.equalsIgnoreCase("very positive")) 
				svalue += 2;
			else if(sentiment.equalsIgnoreCase("positive")) 
				svalue += 1;
			else if(sentiment.equalsIgnoreCase("very negative")) 
				svalue += -2;
			else if(sentiment.equalsIgnoreCase("negative")) 
				svalue += -1;
		}

		// emit hashtag entry
		for(HashtagEntity hashtag : tweet.getHashtagEntities()) {
			LossyEntry entry = new LossyEntry(hashtag.getText(), 1, 0, svalue/sentences.size());
			collector.emit("hashtag-stream", new Values(entry));
		}

		// emit named entity entry
		List<Triple<String,Integer,Integer>> triples = classifier.classifyToCharacterOffsets(tweet.getText());
		for (Triple<String,Integer,Integer> trip : triples) {
			String namedEnt = tweet.getText().substring(trip.second(), trip.third());
			LossyEntry entry = new LossyEntry(namedEnt, 1, 0, svalue/sentences.size());
			this.collector.emit("namedent-stream", new Values(entry));
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("hashtag-stream", new Fields("entry"));
		declarer.declareStream("namedent-stream", new Fields("entry"));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}


}
