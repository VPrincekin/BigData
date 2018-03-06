
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class MainTest {
	
    public static void main(String[] args){
    	TopologyBuilder topo=new TopologyBuilder();
		topo.setSpout("myspout", new MySpout());
		//49  25  24      num *  并行度
		topo.setBolt("mybolt", new SumBolt(),2).shuffleGrouping("myspout");
		topo.setBolt("mybolt", new SumBolt(),2).noneGrouping("myspout");
		topo.setBolt("mybolt", new SumBolt(),2).fieldsGrouping("myspout", new Fields("log"));
		topo.setBolt("mybolt", new SumBolt(),2).allGrouping("myspout");

		//在本地运行
		LocalCluster localCluster=new LocalCluster();
		Config conf=new Config();
		localCluster.submitTopology("mytopo",conf , topo.createTopology());

		//提交到集群
		try {
			StormSubmitter.submitTopology("mytopo",new Config() , topo.createTopology());
		} catch (AlreadyAliveException | InvalidTopologyException e) {
			e.printStackTrace();
		}
    }
}
