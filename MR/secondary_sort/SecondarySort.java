package MR.secondary_sort;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SecondarySort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://hadoop02:9000");
//		System.setProperty("HADOOP_USER_NAME", "hadoop");
        Job job = Job.getInstance(conf);
        job.setJarByClass(SecondarySort.class);

        job.setMapperClass(SecondaryMapper.class);
        job.setReducerClass(SecondaryReduce.class);
        //指定自定义分区和比较器
        job.setPartitionerClass(SecondPartitioner.class);
        job.setSortComparatorClass(SecondGroupComparator.class);

        job.setMapOutputKeyClass(PariWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path input = new Path("input");
        Path output = new Path("output");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output)){
            fs.delete(output,true);
        }

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);

        boolean boo = job.waitForCompletion(true);
        System.exit(boo?0:1);
    }

    public static class SecondaryMapper extends Mapper<LongWritable,Text,PariWritable,IntWritable>{
        private PariWritable mapOutkey = new PariWritable();
        private IntWritable mapOutValue = new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            //设置组合key和value ==> <(key,value),value>
            mapOutkey.setFirst(strs[0]);
            mapOutkey.setSecond(Integer.valueOf(strs[1]));
            mapOutValue.set(Integer.valueOf(strs[1]));
            context.write(mapOutkey,mapOutValue);
        }
    }

    public static class SecondaryReduce extends Reducer<PariWritable,IntWritable,Text,IntWritable>{
        private Text outPutKey = new Text();
        @Override
        protected void reduce(PariWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for(IntWritable value:values){
                outPutKey.set(key.getFirst());
                context.write(outPutKey,value);
            }
        }
    }
}
