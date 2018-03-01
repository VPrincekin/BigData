package MR.job2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//共同好友计算
public class JobMR {
	public static void main(String[] args) throws Exception {
		//第一个job任务
		Configuration conf1=new Configuration();
		Job job1=Job.getInstance(conf1);
		job1.setJarByClass(JobMR.class);
		job1.setMapperClass(JobMRMapper.class);
		job1.setReducerClass(JobMRReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path("c:/job/input"));
		FileOutputFormat.setOutputPath(job1, new Path("c:/job/output"));
		//第二个job任务
		Configuration conf2=new Configuration();
		Job job2=Job.getInstance(conf2);
		job2.setJarByClass(JobMR.class);
		job2.setMapperClass(JobMR2Mapper.class);
		job2.setReducerClass(JobMR2Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path("c:/job/output"));
		FileOutputFormat.setOutputPath(job2, new Path("c:/job/output2"));
		ControlledJob cj1 = new ControlledJob(conf1);
		ControlledJob cj2 = new ControlledJob(conf2);
		cj1.setJob(job1);
		cj2.setJob(job2);
		//设置任务间的依赖关系
		cj2.addDependingJob(cj1);
		//通过JobControl去管理一组有依赖关系的任务的执行
		JobControl jc = new JobControl("CJ");
		jc.addJob(cj1);
		jc.addJob(cj2);
		//提交任务
		Thread thread= new Thread(jc);
		thread.start();
		//判断任务执行的完成情况
		while(!jc.allFinished()){
			Thread.sleep(3000);
		}
		System.out.println("成功");
		//关闭任务线程
		jc.stop();
	}
	
	//第一个MapReduce
	public static class JobMRMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(":");
			String user = split[0];
			String friendStr = split[1];
			String[] friends = friendStr.split(",");

			for (String friend : friends) {
				context.write(new Text(friend), new Text(user));
			}
		}
	}

	public static class JobMRReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			for (Text t : values) {
				sb.append(t.toString()).append("-");
			}
			String friend = sb.toString().substring(0,
					sb.toString().length() - 1);

			context.write(key, new Text(friend));
		}
	}
	//第二个MapReduce
	public static class JobMR2Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] split = value.toString().split("\t");
			String user = split[0];
			String friendStr = split[1];

			String[] friends = friendStr.split("-");
			Arrays.sort(friends);

			for (int i = 0; i < friends.length - 1; i++) {
				for (int j = i + 1; j < friends.length; j++) {
					context.write(new Text(friends[i] + "-" + friends[j]),
							new Text(user));
				}
			}
		}
	}
	public static class  JobMR2Reducer extends Reducer<Text, Text,Text, Text >{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for(Text t: values){
				sb.append(t.toString()).append("-");
			}
			String friend = sb.toString().substring(0, sb.toString().length() - 1);
			
			context.write(key, new Text(friend));
		}
	}
}
