package MR.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
/**
 * 小表与小表join
 * */
public class ReduceJoinMR {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
//		conf.set("fs.defaultFS","hdfs://hadoop2:9000");
//		System.setProperty("HADOOP_USER_NAME", "hadoop");
        Job job = Job.getInstance(conf);
        job.setJarByClass(ReduceJoinMR.class);


    }
    public static class ReduceJoinMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取到当前InputSplit路径
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String name = fileSplit.getPath().getName();

            String[] strs = value.toString().split("\t");
            //根据读到的文件不同区分
            if(name.equals("movies.dat")){
                String movieId = strs[0];
                String movieStr = strs[1]+"\t"+strs[2];
                context.write(new Text(movieId),new Text(name+"::"+movieStr));
            }else{
                String movieId = strs[1];
                String movieStr = strs[0]+"\t"+strs[2]+"\t"+strs[3];
                context.write(new Text(movieId),new Text(name+"::"+movieStr));
            }
        }
    }

    public static class ReduceJoinReduce extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> movieList = new ArrayList<>();
            List<String> ratingList = new ArrayList<>();
            for(Text t:values){
                String[] strs = t.toString().split("::");
                //判断读到的是哪个文件的
                if(strs[0].equals("movies.dat")){
                    movieList.add(strs[1]);
                }else {
                    ratingList.add(strs[1]);
                }

            }
            //拿到movieList和ratingList做笛卡尔积
            int ml = movieList.size();
            int rl = ratingList.size();
            for(int i=0;i<ml;i++){
                for(int j=0;j<rl;j++){
                    String keyOut = key.toString()+"::"+movieList.get(i)+"::"+ratingList.get(j);
                    context.write(new Text(keyOut),NullWritable.get());
                }
            }
        }
    }

}
