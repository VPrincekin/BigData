package MR.sum_sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**实现自定义的bean来封装流量信息，将bean作为map输出的key来传输MR。
 * MR程序在处理数据的过程中会对数据排序(map输出的kv对传输到reduce之前，会排序)
 * 排序的依据是map输出的key,所以，我们如果要实现自己需要的排序规则，可以考虑将排序因素放到key中
 * 让key实现 WritableComparable 接口，然后重写key的compareTo方法。
*
* */

public class FlowMRSortSum {
	public static void main(String[] args) throws Exception {
		//指定mapreduce运行的hdfs相关的参数
		Configuration conf= new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop2:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		//获取Job对象
		Job job = Job.getInstance(conf);
		
		//设置运行环境
		job.setJarByClass(FlowMRSum.class);
//		job.setJar("/home/hadoop/jar/flowmr.jar");
		
		//指定mapper类和reduce类
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReduce.class);
		
		//指定maptask的输出key-value类型和reducetask的输出key-value类型。
		//如果两者输出的类型错误相同，可以不用写maptask的。
		job.setOutputKeyClass(SortFlowBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		//指定输入输出的路径。
		Path inputPath = new Path("/flow/output");
		Path outPath = new Path("/flow/output_sort");
		//判断输出的文件夹是否已经存在，若存在删除 
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath,true);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPath);
		
		//最后提交任务
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion?0:1);
	}
	public static class SortMapper extends Mapper<LongWritable, Text,SortFlowBean, NullWritable>{
		@Override
		protected void map(LongWritable key,Text value,Context context)
				throws IOException, InterruptedException {
				String[] splits = value.toString().split("\t");
				SortFlowBean sfb=new SortFlowBean(splits[0],Long.parseLong(splits[1]),Long.parseLong(splits[2]));
				context.write(sfb, NullWritable.get());
		}
	}
	public static class SortReduce extends Reducer<SortFlowBean, NullWritable, SortFlowBean, NullWritable>{
		@Override
		protected void reduce(SortFlowBean sfb,Iterable<NullWritable> values,Context context)
				throws IOException, InterruptedException {
			
			context.write(sfb, NullWritable.get());
		}
	}
}
