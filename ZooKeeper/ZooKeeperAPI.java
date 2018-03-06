package wdh.zk.exercise;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperAPI {
	private static String connectString="hadoop2:2181";
	private static int sessionTimeout=5000;
	private static List<ACL> acl = Ids.OPEN_ACL_UNSAFE;
	public static void main(String[] args) throws Exception {
		ZooKeeper zk = new ZooKeeper(connectString, sessionTimeout, null);
		CreateMode createMode = CreateMode.PERSISTENT;
		String path="/zk";
		String value="999";
		String newValue="888";
//		createZnode(path, value, zk, acl, createMode);
//		getZnodeData(path, zk,false,null);
//		updateZnodeData(path, newValue, zk);
//		listChildren(path, zk);
		
		rmr(path, zk);
		
		zk.close();
	}
	//创建一个节点(返回值是节点的路径)
	public static void createZnode(String path,String value,ZooKeeper zk,List<ACL> acl,CreateMode createMode ) throws Exception{
		String createPath = zk.create(path, value.getBytes(), acl, createMode);
		System.out.println(createPath);
	}
	//获取节点的数据
	public static void getZnodeData(String path,ZooKeeper zk,boolean watch,Stat stat) throws Exception{
		byte[] data = zk.getData(path, watch, stat);
		System.out.println(new String(data));
	}
	//修改节点数据(返回值是更新时间戳)
	public static void updateZnodeData(String path,String newValue ,ZooKeeper zk)throws Exception{
		Stat setData = zk.setData(path, newValue.getBytes(), -1);
		long ctime = setData.getCtime();
		System.out.println(ctime);
	}
	//查看某个节点下的孩子节点列表
	public static void listChildren(String path,ZooKeeper zk) throws Exception{
		List<String> children = zk.getChildren(path, false);
		for(String ch : children){
			System.out.println(path+"/"+ch);
		}
	}
	//级联删除
	public static void rmr(String path,ZooKeeper zk)throws Exception{
		System.out.println("开始对此节点进行判断"+path);
		List<String> children = zk.getChildren(path, false);
		if(children.size()>0){
			for(String ch : children){
				if(zk.exists(path+"/"+ch, null) != null){
					rmr(path+"/"+ch,zk);
				}
			}
		}else{
			zk.delete(path, -1);
			System.out.println("删除此节点"+path);
			int index=path.lastIndexOf("/");
			System.out.println(index);
			if(index!=0){
				String returnStr=path.substring(0,index);
				System.out.println("重新对此节点进行判断"+returnStr);
				rmr(returnStr,zk);
			}else{
				System.out.println("全部删除成功！！！");
			}
		}
	}
}
