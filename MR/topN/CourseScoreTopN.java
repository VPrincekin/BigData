package MR.topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CourseScoreTopN {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(CourseScoreTopN.class);

    }
    public static class topMapper extends Mapper<LongWritable,Text,CourseScore,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            String coure = strs[0];
            String name = strs[1];
            double sum =0;
            for(int i=2;i<strs.length;i++){
                sum += Double.parseDouble(strs[i]);
            }
            double avg = sum/(strs.length-2);
            CourseScore cs = new CourseScore(coure, name, avg);
            context.write(cs,NullWritable.get());
        }
    }

    public static class CourseScoreComparator extends WritableComparator{
        CourseScoreComparator(){
            super(CourseScore.class,true);
        }
        @Override
        public int compare(Object a, Object b) {
            CourseScore csa = (CourseScore) a;
            CourseScore csb = (CourseScore) b;
            int compare = csa.getCoure().compareTo(csb.getCoure());
            return compare;
        }
    }

    public static class topReduce extends Reducer<CourseScore,NullWritable,CourseScore,NullWritable>{
        int topN = 3;
        @Override
        protected void reduce(CourseScore key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for(NullWritable nw:values){
                context.write(key,NullWritable.get());
                i++;
                if(i==topN){
                    break;
                }
            }
        }
    }

}
