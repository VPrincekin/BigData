package HBase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class HDFS_TO_HBaseMR {
    //设置zookeeper和hdfs的连接
    public static final String ZOOKEEPER_CONNECT_KEY="hbase.zookeeper.quorum";
    public static final String ZOOKEEPER_CONNECT="hadoop2:2181,hadoop3:2181,hadoop4:2181";
    public static final String HDFS_CONNECT_KEY="fs.defaultFS";
    public static final String HDFS_CONNECT="hdfs://myha01/";
    public static final String Table_Name = "person_info";
    public static final String Family_Name="base_info";
    public static void main(String[] args) throws Exception {
        //配置管理
        Configuration conf = HBaseConfiguration.create();
        conf.set(ZOOKEEPER_CONNECT_KEY, ZOOKEEPER_CONNECT);
        conf.set(HDFS_CONNECT_KEY, HDFS_CONNECT);
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Job job = Job.getInstance(conf);

        job.setMapperClass(HDFSDataToHBaseMRMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //判断hbase表存不存在，不存在创建
        Connection connect = ConnectionFactory.createConnection(conf);
        Admin admin = connect.getAdmin();
        boolean tableExists = admin.tableExists(TableName.valueOf(Table_Name));
        if(!tableExists){
            //指定表名
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(Table_Name));
            //指定列簇名
            htd.addFamily(new HColumnDescriptor(Family_Name.getBytes()));
            admin.createTable(htd);
        }
        TableMapReduceUtil.initTableReducerJob(Table_Name,HDFSDataToHBaseMRReduce.class,job);
        //输入数据来源于hdfs
        FileInputFormat.addInputPath(job, new Path("/hbase2hdfs/output1"));

        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion?0:1);
    }
    static class HDFSDataToHBaseMRMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }
    static class HDFSDataToHBaseMRReduce extends TableReducer<Text, NullWritable, ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key,Iterable<NullWritable> values,Context context)
                throws IOException, InterruptedException {
            //做一次循环遍历，为了防止key产生重复而造成数据丢失
            for(NullWritable nw:values){
                String[] split = key.toString().split("\t");
                String rowkeyStr = split[0];
                ImmutableBytesWritable rowkey = new ImmutableBytesWritable(rowkeyStr.getBytes());
                byte[] family = split[1].getBytes();//列簇
                byte[] qualifier = split[2].getBytes();//列名
                byte[] value = split[3].getBytes();//值
                Long ts = Long.parseLong(split[4]);
                Put put = new Put(rowkeyStr.getBytes());
                put.addColumn(family, qualifier, ts, value);
                context.write(rowkey, put);
            }
        }
    }
}
