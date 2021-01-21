package org.opt.mean;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MeanMapper extends
		Mapper<LongWritable, Text, IntWritable, TwovalueWritable> {

	HashMap<Integer, Double> sumVal = new HashMap<Integer, Double>();
	int n = 0;
	
	//Reuse
	IntWritable keyEmit = new IntWritable();
	TwovalueWritable valEmit = new TwovalueWritable();

	public void map(LongWritable key, Text value, Context context) {
		
		n++; // total count
		Configuration conf = context.getConfiguration();
		String delim = conf.get("delimiter");
		//Get each line
		String line = value.toString();
		
		HashMap<Integer, Double> mapLine = new HashMap<Integer, Double>();
		String parts[] = line.split(delim);
		for (int i = 0; i < parts.length; i++) {
			//storing each line in a hashmap; can use mapWritable also
			mapLine.put(i + 1, Double.parseDouble(parts[i]));
		}
		
		//Calculating sum
		if (sumVal.isEmpty()) {
			//if sumval is empty add elements to sumval
			sumVal.putAll(mapLine);
		} else {
			//calculating sum
			double sum = 0;
			for (Integer colId : mapLine.keySet()) {
				double val1 = mapLine.get(colId);
				double val2 = sumVal.get(colId);
				/*
				 * calculating sum
				 */
				sum = val1 + val2;
				sumVal.put(colId, sum);
			}
		}

	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		for (int colId : sumVal.keySet()) {
			double val1 = sumVal.get(colId);
			keyEmit.set(colId);
			valEmit.set(val1, n);
			//Emits column id as key
			//Emits sum and total line processed as value
			context.write(keyEmit, valEmit);
		}
	}
}
