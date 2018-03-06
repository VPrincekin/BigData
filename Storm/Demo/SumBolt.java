
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SumBolt extends  BaseBasicBolt {
	int sum=0;
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str=input.getStringByField("log");
		if(str != null){
			sum++;
		}
		System.err.println(Thread.currentThread().getName() + sum);
	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
