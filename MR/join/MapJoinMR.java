package MR.join;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * 大表与小表join
 * */
public class MapJoinMR {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://hadoop2:9000");
//        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapJoinMR.class);

        job.setMapperClass(MapJoinMapper.class);
        job.setReducerClass(MapJoinReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //mapjoin场景中的小表，这些小表的数据都要被全部加载到内存当中
        URI movieURI = new URI("hdfs://hadoop2:9000/join/input/movie/movies.dat");
        URI userURI = new URI("hdfs://hadoop2:9000/join/input/user/users.dat");
        URI[] uris = new URI[]{movieURI,userURI};
        job.setCacheFiles(uris);

        Path ratingPath = new Path("");
        Path outputPath = new Path("");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job,ratingPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        boolean boo = job.waitForCompletion(true);
        System.exit(boo?0:1);
    }

    public static  class MapJoinMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        private static Map<String,String>  movieMap = new HashMap<>();
        private static Map<String, String> userMap = new HashMap<>();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //在此做循环操作，循环的是大表当中的一个Inputsplit的数据
            String[] strs = value.toString().split("\t");
            String userID = strs[0];
            String movieID = strs[1];
            String ratingStr = strs[0]+"::"+strs[2]+"::"+strs[3];
            String movieStr = movieMap.get(movieID);
            String userStr = userMap.get(userID);
            String keyOut = movieID +"::"+movieStr+"::"+ratingStr+"::"+userStr;
            context.write(new Text(keyOut),NullWritable.get());

        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] localCachaFiles = context.getLocalCacheFiles();
            String moviePath = localCachaFiles[0].toUri().toString();
            String userPath = localCachaFiles[1].toUri().toString();
            BufferedReader movieBR = new BufferedReader(new FileReader(new File(moviePath)));
            BufferedReader userBR = new BufferedReader(new FileReader(new File(userPath)));
            String line = null;
            while((line=movieBR.readLine())!=null){
                String[] strs = line.split("\t");
                String movieID = strs[0];
                String movieStr = strs[1]+"::"+strs[2];
                movieMap.put(movieID,movieStr);

            }
            while ((line = userBR.readLine())!=null){
                String[] split = line.split("::");
                String userId = split[0];
                String userStr=split[1]+"::"+split[2]+"::"+split[3]+"::"+split[4];
                userMap.put(userId, userStr);
            }
            movieBR.close();
            userBR.close();
        }
    }

    public static class MapJoinReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values,Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
}
