package org.opt.mean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new Driver(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.out
			.printf("Usage: Driver <input dir> <output dir> \n");
			System.exit(-1);
		}
		/*
		 * Configuration objects
		 */
		Configuration conf = new Configuration();
		//Custom Delimiter
		conf.set("delimiter", args[2]);
		
		Job job = new Job(conf, "Optimizing Mean");
		job.setJarByClass(Driver.class);
		FileSystem fs = FileSystem.get(conf);

		//Input and Output paths
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TwovalueWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(TwovalueWritable.class);
		//Mapper and Reducer class
		job.setMapperClass(MeanMapper.class);
		job.setReducerClass(MeanReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//Delete output directory if exist
		if(fs.exists(out)){
			fs.delete(out, true);
		}
		//Input and output path through command line
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);

	}
}
