package MR.secondary_sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * 自定义数据类型关键代码
 * */
public class PariWritable implements WritableComparable<PariWritable>{
    //组合Key
    private String first;
    private int second;
    public PariWritable(){}
    public PariWritable(String first,int second){
        this.first=first;
        this.second=second;

    }
    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
    /**
     *反序列化
    * */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readUTF();
        this.second = in.readInt();
    }
    /**
     * 序列化
     * */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeInt(second);

    }
    /**重写比较器
    * */
    @Override
    public int compareTo(PariWritable o) {
        int comp = this.first.compareTo(o.first);
        if(comp!=0){
            return comp;
        }else{//若第一个字段相等，则比较第二个字段
            return Integer.valueOf(this.second).compareTo(Integer.valueOf(o.second));
        }
    }
}
