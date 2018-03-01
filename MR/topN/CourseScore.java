package MR.topN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CourseScore implements WritableComparable<CourseScore> {
    private String coure;
    private String name;
    private double score;
    public CourseScore(){}
    public CourseScore(String coure, String name, double score) {
        this.coure = coure;
        this.name = name;
        this.score = score;
    }

    public String getCoure() {
        return coure;
    }

    public void setCoure(String coure) {
        this.coure = coure;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public int compareTo(CourseScore cs) {
        int compare = cs.coure.compareTo(this.coure);
        if(compare==0){
            double d = cs.score - this.score;
            if(d>0){
                return 1;
            }else if(d<0){
                return -1;
            }else
                return 0;
        } else
            return compare;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(coure);
        out.writeUTF(name);
        out.writeDouble(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.coure = in.readUTF();
        this.name = in.readUTF();
        this.score = in.readInt();
    }
}
