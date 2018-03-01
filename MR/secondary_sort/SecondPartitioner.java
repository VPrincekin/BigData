package MR.secondary_sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * 自定义分区规则
 * */
public class SecondPartitioner extends Partitioner<PariWritable,IntWritable>{

    @Override
    public int getPartition(PariWritable key, IntWritable intWritable, int numPartitions) {
        /*
         * 默认的实现 (key.hashCode() & Integer.MAX_VALUE) % numPartitions
         * 让key中first字段作为分区依据
         */
        return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;

//      return Math.abs(key.getFirst() * 127) % numPartitions;  //first为数值类型时使用
    }
}

class PMRPartitioner extends Partitioner<Text, Text> {
    private static HashMap<String,Integer> courseMap=new HashMap<String,Integer>();
    static{
        courseMap.put("computer", 0);
        courseMap.put("english", 1);
        courseMap.put("algorithm", 2);
        courseMap.put("math", 3);
    }
    @Override
    public int getPartition(Text key, Text value, int numReducetask) {
        Integer code=courseMap.get(key.toString());
        return code;
    }

}