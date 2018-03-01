package MR.sum_sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**计算单个手机号的流量总和，结果用于排序
 * */
public class FlowMRSum {
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop2:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Job job = Job.getInstance(conf);
		
//		job.setJarByClass(FlowMRSum.class);
		job.setJar("/home/hadoop/jar/flowmr.jar");
		
		job.setMapperClass(SumMapper.class);
		job.setReducerClass(SumReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("/flow/input"));
		FileOutputFormat.setOutputPath(job, new Path("/flow/output"));
		
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion?0:1);
	
	}
	
	public static class SumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] splits = value.toString().split("\t");
			int length=splits.length;
			String telephone=splits[1];
			String upFlow=splits[length-3];
			String downFlow=splits[length-2];
			FlowBean fb=new FlowBean(Long.parseLong(upFlow),Long.parseLong(downFlow));
			context.write(new Text(telephone), fb);
		}
	}
	public static class SumReduce extends Reducer<Text, FlowBean, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<FlowBean> flowBeans,Context context)
				throws IOException, InterruptedException {
			
			long sumUpFlow=0;//总上行流量
			long sumDownFlow=0;//总下行流量
			for(FlowBean fb:flowBeans){
				sumUpFlow+=fb.getUpFlow();
				sumDownFlow+=fb.getDownFlow();
			}
			FlowBean resultFB=new FlowBean(sumUpFlow,sumDownFlow);
			context.write(key, new Text(resultFB.toString()));
		}
	}
	
}
