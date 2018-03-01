package MR.secondary_sort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组比较器
* */
public class SecondGroupComparator implements RawComparator<PariWritable> {
    /*
    *字节比较
    * */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return WritableComparator.compareBytes(b1,s1,Integer.SIZE/8,b2,s2,Integer.SIZE/8);

    }
    /*
    *对象比较
    */
    @Override
    public int compare(PariWritable o1, PariWritable o2) {
        return o1.getFirst().compareTo(o2.getFirst());
    }
}
