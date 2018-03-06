package wdh.zk.conf;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 配置管理：把集群的所有或者关键信息都存放在zookeeper的文件系统
 * 配置修改程序（client）--在zookeeper的文件系统里面增加，删除，或者修改配置信息。
 * 配置修改程序UpdateConfig（增加，删除，修改）（父节点：/config） 核心思路：
 * 1、拿zookeeper连接
 * 2.1	增加配置程序
 * 2.2	删除配置程序
 * 2.3	修改配置程序
 * 3	关闭zookeeper连接
 * 做配置管理：key-value
 * 重点问题：key当做节点名称，value当做节点的数据
 * */
public class UpdateConfig {
	public static final String connectString="hadoop2:2181,hadoop3:2181,hadoop4:2181";
	public static final int sessionTimeout=5000;
	public static final String ParentNode="/config";
	public static void main(String[] args) throws Exception{
		ZooKeeper zk = new ZooKeeper(connectString, sessionTimeout, null);
//		addConfig("hadoop3", "111", zk);
		deleteConfig("hadoop3", zk);
//		setConfig("hadoop3", "333", zk);
	}
	//模拟增加配置
	public static void addConfig(String key,String value,ZooKeeper zk) throws Exception{
		zk.create(ParentNode+"/"+key,value.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("增加配置成功");
	}
	//模拟删除配置
	public static void deleteConfig(String key,ZooKeeper zk) throws Exception{
		zk.delete(ParentNode+"/"+key, -1);
		System.out.println("删除配置成功");//没有加判断（是否存在？）
	}
	//模拟修改配置
	public static void setConfig(String key,String value,ZooKeeper zk) throws Exception{
		Stat setData = zk.setData(ParentNode+"/"+key, value.getBytes(), -1);
		System.out.println("配置修改成功");//没有加判断（和原数据是否相同？）
	}
}
