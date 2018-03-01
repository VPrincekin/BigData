package MR.wordcount;


import org.apache.hadoop.conf.Configuration;
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

public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//      conf.set("fs.defaultFS", "hdfs://hadoop2:9000");
//      System.setProperty("HADOOP_USER_NAME","hadoop");
//        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf);

//      指定yarn去找mapreduce程序在哪个jar包
//      job.setJar("/home/hadoop/jar/wordcount.jar");
        //本地运行
        job.setJarByClass(WordCount.class);

        // 指定mapper类和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCounReduce.class);

        //设置map阶段输出的key-value 类型。
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce阶段的key-value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定文件的输入输出路径
        FileInputFormat.addInputPath(job,new Path("D:/wordcount/input"));
        FileOutputFormat.setOutputPath(job,new Path("D:/wordcount/output"));

//        //map阶段输入的文件路径
//        Path inputPath = new Path("hdfs://hadoop2:9000/wordcount/input");
//        //reduce阶段输出的文件路径
//        Path outPath = new Path("hdfs://hadoop2:9000/wordcount/output5");
//
//        FileInputFormat.addInputPath(job, inputPath);
//        FileOutputFormat.setOutputPath(job, outPath);
//
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion?0:1);


    }
    /**KEYIN:mapreduce程序从输入文件里面逐行读数据的时候每一行的起始偏移量，是一个long类型的数值。
     VALUEIN:mapreduce程序从输入文件里面逐行读数据的时候每一行的内容。

     (hello,1),(tom,1),......keyout代表的就是hello,tome等这种单词的类型在mapreduce框架中的序列化类型。
     valueout 代表的就是1,1,1......等这些数据的原始类型在mapreduce框架中的序列化类型。
     KEYOUT: map方法处理过后的key-value对的key的类型。
     VALUEOUT: map方法处理过后的key-value对的value的类型。*/
    public static  class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拿到文本值value做切割
            String[] words = value.toString().split(",");
            for(String word : words){
                //输出到Reduce
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }

    public static class WordCounReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        /**
         * key:当前reduce方便被调用一次时所传进来的key-value对的key
         * values:里面的值就是一个key所对应的所有的values值的集合。
         * */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable num:values){
                sum += num.get();
            }
            //输出最后的结果
            context.write(key,new IntWritable(sum));
        }
    }
}
