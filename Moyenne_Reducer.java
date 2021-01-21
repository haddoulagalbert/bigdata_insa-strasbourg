package org.opt.mean;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class MeanReducer extends
		Reducer<IntWritable, TwovalueWritable, IntWritable, DoubleWritable> {
	DoubleWritable valEmit = new DoubleWritable();

	public void reduce(IntWritable key, Iterable<TwovalueWritable> values,
			Context context) throws IOException, InterruptedException {
		double sum = 0;
		double mean = 0;
		int total = 0;
		//Iterating through values for each key
		for (TwovalueWritable value : values) {
			//Taking sum of values and total number of lines 
			sum += value.getSum();
			total += value.getTotalCnt();
		}
		//sum contains total sum of all elements in each column
		//total contains total no of elements in each column
		mean = sum / total;
		valEmit.set(mean);
		context.write(key, valEmit);
	}

}
