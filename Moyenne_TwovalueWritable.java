package org.opt.mean;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


public class TwovalueWritable implements Writable {
	private double sum;	//total sum
	private int totalCnt;	//total no of lines


	public TwovalueWritable() {
		super();
	}
	public  TwovalueWritable(double sum, int totalCnt) {
		set(sum, totalCnt);
	}
	public void set(double sum, int totalCnt) {
		this.sum = sum;
		this.totalCnt = totalCnt;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(sum);
		out.writeInt(totalCnt);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		sum = in.readDouble();
		totalCnt = in.readInt();
	}

	/**
	 * @return the sum
	 */
	public double getSum() {
		return sum;
	}
	/**
	 * @param sum the sum to set
	 */
	public void setSum(double sum) {
		this.sum = sum;
	}
	/**
	 * @return the totalCnt
	 */
	public int getTotalCnt() {
		return totalCnt;
	}
	/**
	 * @param totalCnt the totalCnt to set
	 */
	public void setTotalCnt(int totalCnt) {
		this.totalCnt = totalCnt;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(sum);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + totalCnt;
		return result;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof TwovalueWritable)) {
			return false;
		}
		TwovalueWritable other = (TwovalueWritable) obj;
		if (Double.doubleToLongBits(sum) != Double.doubleToLongBits(other.sum)) {
			return false;
		}
		if (totalCnt != other.totalCnt) {
			return false;
		}
		return true;
	}
	@Override
	public String toString() {
		return sum + " " + totalCnt;
	}
}
