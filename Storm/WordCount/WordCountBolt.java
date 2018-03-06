
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WordCountBolt extends BaseRichBolt {
	/**
	 * 1) 首先去除上一次的值
	 * 2） 做累加
	 * 3） 放回去
	 * 
	 * rowkey word
	 * cf:count  count
	 * 
	 * 
	 * update  
	 * 
	 * reids hbase
	 */
	Map<String,Integer> counts=new HashMap<String,Integer>();
	// 放到持久化的系统

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

	}
  
	@Override
	public void execute(Tuple input) {
		String word=input.getStringByField("word");
		Integer count=counts.get(word);
		if(count == null){
			count=0;
		}
		count++;
		counts.put(word, count);
		
		for(String key:counts.keySet()){
			System.err.println(key + " -> "+counts.get(key));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
