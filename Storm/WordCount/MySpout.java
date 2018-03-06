
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import jline.internal.InputStreamReader;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
/**
 * 
 * extends  BaseRichSpout  抽象类
 * 
 * 
 * implements IRichSpout 接口
 * @author admin
 *
 */
public class MySpout implements IRichSpout {
	BufferedReader br=null;
	SpoutOutputCollector collector;
	
	/**
	 * 初始化
	 */
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector=collector;
		try {
			 br=new BufferedReader(new InputStreamReader(new FileInputStream(new File("file.txt"))));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 释放资源
	 */
	@Override
	public void close() {
		try {
			if(br != null){
			br.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void activate() {

	}

	@Override
	public void deactivate() {

	}
	/**
	 * 核心的逻辑
	 */
	@Override
	public void nextTuple() {
		String str=null;
		try {
			if((str=br.readLine())  != null){
				this.collector.emit(new Values(str));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 数据处理成功以后，会被调用！！
	 */
	@Override
	public void ack(Object msgId) {

	}
	/**
	 * 数据处理失败以后，会被调用
	 */
	@Override
	public void fail(Object msgId) {
		
		
	}
	/**
	 * 声明，定义发到下一级tuple 的别名
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log"));
		
	}
    /**
     * 获取配置文件阐述
     */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	

}
