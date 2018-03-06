package lesson03;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {
	public static void main(String[] args){
		TopologyBuilder topo=new TopologyBuilder();
		topo.setSpout("dataspout", new MySpout());
		topo.setBolt("splitbolt", new SplitBolt(),3).shuffleGrouping("dataspout");
		topo.setBolt("wordcountbolt", new WordCountBolt(),2).fieldsGrouping("splitbolt", new Fields("word"));
		
		LocalCluster local= new LocalCluster();
		local.submitTopology("test", new Config(), topo.createTopology());
	}

}
