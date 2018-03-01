package MR.sum_sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SortFlowBean implements WritableComparable<SortFlowBean>{
	private String telephone;
	private long upFlow;
	private long downFlow;
	private long sumFlow;
	public String getTelephone() {
		return telephone;
	}
	public void setTelephone(String telephone) {
		this.telephone = telephone;
	}
	public long getUpFlow() {
	    return upFlow;
	}
	public void setUpFlow(long upFlow) {
	    this.upFlow = upFlow;
	}
	public long getDownFlow() {
		return downFlow;
	}
	public void setDownFlow(long downFlow) {
	    this.downFlow = downFlow;
	}
	public long getSumFlow() {
	    return sumFlow;
	}
	public void setSumFlow(long sumFlow) {
	    this.sumFlow = sumFlow;
	}
	@Override
	public String toString() {
		return telephone + "\t" + upFlow+ "\t" + downFlow + "\t" + sumFlow ;
	}
	public SortFlowBean() {
		super();
	}
	public SortFlowBean(String telephone, long upFlow, long downFlow) {
		super();
		this.telephone = telephone;
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow=upFlow+downFlow;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(telephone);
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.telephone=in.readUTF();
		this.upFlow=in.readLong();
		this.downFlow=in.readLong();
		this.sumFlow=in.readLong();
	}
	@Override
	public int compareTo(SortFlowBean sfb) {
		long result=this.getSumFlow()-sfb.getSumFlow();
		if (result>0){
			return 1;
		} else if(result==0){
			return 0;
		} else {
			return -1;
		}
	}
}
