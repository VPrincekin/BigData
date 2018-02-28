package hdfs.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class HDFSFileStream {
	private static FileSystem fs=null;
	public static void main(String[] args) throws Exception {
		URI uri = new URI("hdfs://hadoop2:9000");
		String user="hadoop";
		//cof里面管理的是集群的所有配置信息，包括我们在配置文件里面的配置信息
		Configuration conf=new Configuration();
		//如果我们想获取HDFS集群的配置信息，那么就要指定fs.defaultFS这个
		//让conf知道它所管理的集群信息到底是哪个文件系统。
		conf.set("fs.defaultFS","hdfs://hadoop2:9000");
		//指定用户
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		conf.addResource("hdfs-default.xml");
		String dfsName=conf.get("fs.defaultFS");
		String bz = conf.get("dfs.blocksize");
		System.out.println(dfsName+"\t"+bz);
		fs=FileSystem.get(conf);
		//使用流的方式上传文件
//		uploadByStream();
		//使用流的方式下载文件
		downloadByStream();
		fs.close();
	}
	//使用原生流的方式上传文件
	public static void uploadByStream() throws Exception{
		//输入流：从本地指定上传文件。
		FileInputStream fis = new FileInputStream(new File("C:\\2017.4.24.rar"));
		//输出流：hdfs指定目录
		FSDataOutputStream fsdout = fs.create(new Path("/2017.4.24.rar"));
		//输入流与输出流的交互
		IOUtils.copyBytes(fis,fsdout,4096);
	}
	//使用原生流的方式去hdfs下载文件
	public static void downloadByStream() throws Exception{
		//输入流：从hdfs指定文件
		FSDataInputStream fsdin =fs.open(new Path("/2017.4.24.rar"));
		//输出流：本地指定目录
		FileOutputStream fos=new  FileOutputStream(new File("C:/aaa.rar"));
		//输入流与输出流的交互
		IOUtils.copyBytes(fsdin, fos, 4096);
	}
}

