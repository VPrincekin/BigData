package HBase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//编写mapreduce程序从hbase读取数据，然后存储到hdfs
public class HBase_To_HDFSMR {
    public static final String ZK_CONNECT_KEY="hbase.zookeeper.quorum";
    public static final String ZK_CONNECT="hadoop2:2181,hadoop3:2181,hadoop4:2181";
    public static final String HDFS_CONNECT_KEY="fs.defaultFS";
    public static final String HDFS_CONNECT="hdfs://myha01/";
    public static final String Table_Name="user_info";
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set(ZK_CONNECT_KEY, ZK_CONNECT);
        conf.set(HDFS_CONNECT_KEY, HDFS_CONNECT);
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Job job = Job.getInstance(conf);
        //输入数据来源于hbase的user_info表
        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob(Table_Name, scan,
                HBaseDataToHDFSMRMapper.class, Text.class, NullWritable.class, job);
        //数据输出到hdfs
        FileOutputFormat.setOutputPath(job, new Path("/hbase2hdfs/output1"));
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion?0:1);

    }
    /**mapper的输入key-value类型是：ImmutableBytesWritable,Result
     * mapper的输出key-value类型用户自定义
     * */
    static class HBaseDataToHDFSMRMapper extends TableMapper<Text, NullWritable>{
        /** keyType: LongWritable -- ImmutableBytesWritable:rowkey
         * ValueType: Text --  Result:hbase表中某一个rowkey查询出来的所有的key-value对
         */
        @Override
        protected void map(ImmutableBytesWritable key,Result value,Context context)
                throws IOException, InterruptedException {
            String rowkey = Bytes.toString(key.copyBytes());
            List<Cell> listCells = value.listCells();
            Text text = new Text();
            for(Cell cell : listCells){
                String family=new String(CellUtil.cloneFamily(cell));
                String qualifier = new String(CellUtil.cloneQualifier(cell));
                String val = new String(CellUtil.cloneValue(cell));
                long ts = cell.getTimestamp();
                text.set(rowkey+"\t"+family+"\t"+qualifier+"\t"+val+"\t"+ts);
                context.write(text, NullWritable.get());
            }
        }
    }
    static class HBaseDataToHDFSMRReduce extends TableReducer<Text, NullWritable, ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key,Iterable<NullWritable> values,Context context)
                throws IOException, InterruptedException {
        }
    }
}
