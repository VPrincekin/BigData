package wdh.zk.conf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 响应程序（datanode）
 * 从节点的响应程序（修改程序，修改任意一个配置，响应程序都要接到具体详细通知）
 * 监控 /config下面子节点的个数变化（增加和删除）和任意子节点的数据变化（修改）
 * 核心设计思路：
 * 1、拿zookeeper连接
 * 2、判断父节点存在不存在，不存在则创建
 * 3.1 监控/config节点的NodeChildrenChange事件
 * 3.2 监控/config节点下的子节点的节点数据变化NodeDataChanged事件
 * 4、	模拟程序一直运行
 * 5、	关闭zookeeper
 * */
public class RespondConfig {
	public static String connectString="hadoop2:2181,hadoop3:2181,hadoop4:2181";
	public static int sessionTimeout=5000;
	public static String ParentNode="/config";
	public static String ParentValue="www";
	public static ZooKeeper zk= null;
	public static Map<String, String> configMap=null;
	public static void main(String[] args) throws Exception {
			zk=new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				String path=event.getPath();
				EventType type = event.getType();
				System.out.println(path+"----"+type);
				//拿到配置变化之后的所有配置信息
				Map<String , String> currentConfigMap=getMpaConfig(ParentNode, zk);
				//进行NodeChildrenChanged响应之后执行的代码，判断是增加还是删除?
				if(type.equals(EventType.NodeChildrenChanged) && path.equals(ParentNode)){
					//判断是增加配置还是减少配置
					int currentLength = currentConfigMap.size();
					int configLength = configMap.size();
					if(configLength>currentLength){
						//删除了一个配置
						List<String> changeConfig = getChangeConfig(currentConfigMap, configMap);
						System.out.println("删除了一项配置"+changeConfig.get(0)+"::"+changeConfig.get(1));
					}else if(configLength<currentLength){
						//增加了一个配置
						List<String> changeConfig = getChangeConfig(configMap, currentConfigMap);
						//给新增的配置添加监听
						try {
							zk.getData(changeConfig.get(0), true, null);
						} catch (KeeperException | InterruptedException e) {
							e.printStackTrace();
						}
						System.out.println("增加了一个配置"+changeConfig.get(0)+"::"+changeConfig.get(1));
					}
					//响应结束之后，给configMap重新赋值
					configMap=currentConfigMap;
					//连续监控
					try {
						zk.getChildren(ParentNode, true);
					} catch (KeeperException | InterruptedException e) {
						e.printStackTrace();
					}
				}
				//进行NodeDataChanged响应之后执行的代码 （修改子节点的数据）
				//两个条件：子节点的路径和子节点数据变化的事件
				Set<String> keySet = configMap.keySet();
				for(String childNodePath:keySet){
					if(type.equals(EventType.NodeDeleted) && path.equals(childNodePath)){
						System.out.println("删除了一个节点");
					}
					if(type.equals(EventType.NodeDataChanged) && path.equals(childNodePath)){
						//找到了某个节点的数据变化事件，打印出具体变化情况
						String value=configMap.get(childNodePath);
						String currentValue=currentConfigMap.get(childNodePath);
						System.out.println("配置信息::"+childNodePath+"从"+value+"变成了"+currentValue);
						//做到连续监听
						try {
							zk.getData(childNodePath, true, null);
						} catch (KeeperException | InterruptedException e) {
							e.printStackTrace();
						}
						//响应结束之后，给configMap重新赋值
						configMap=currentConfigMap;
					}
				}
			}
		});
		// 先获取/config节点下所有的子节点数据
		configMap=getMpaConfig(ParentNode, zk);
		
		//判断父节点是否存在，不存在创建
		if(zk.exists(ParentNode, null)==null){
			zk.create(ParentNode, ParentValue.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		//对/config节点进行NodeChildrenChanged监控
		zk.getChildren(ParentNode, true);
		//对/config节点下的子节点做监控     ，监控这些节点的数据变化 NodeDataChanged
		Set<String> keySet = configMap.keySet();
		for(String key : keySet){
			//key就是某个节点的绝对路径
			zk.getData(key, true, null);
		}
		//模拟程序一直运行
		Thread.sleep(Integer.MAX_VALUE);
		//关闭zk连接
		zk.close();
	}
	//该方法返回/config节点下面所有子节点的数据（key(节点名称--全路径) value(节点的数据)）
	public static Map<String, String> getMpaConfig(String path,ZooKeeper zk){
		Map<String,String> configMap=new HashMap<String, String>();
		try {
			List<String> children = zk.getChildren(path, null);
			for(String ch:children){
				String outPath=path+"/"+ch;
				byte[] data = zk.getData(outPath, null, null);
				configMap.put(outPath, new String(data));
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		return configMap;
	}
	public static List<String> getChangeConfig(Map<String, String> smallMap,Map<String, String> bigMap){
		Set<String> keySet = bigMap.keySet();
		List<String> resultList=new ArrayList<>();
		for(String key:keySet){
			if(!smallMap.containsKey(key)){
				resultList.add(key);
				resultList.add(bigMap.get(key));
			}
		}
		return resultList;
	}
}
