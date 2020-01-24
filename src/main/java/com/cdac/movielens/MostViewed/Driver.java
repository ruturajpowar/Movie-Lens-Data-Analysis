package com.cdac.movielens.MostViewed;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	public static void main(String[] args) {
		int code = -1;
		try {
			code = ToolRunner.run(new Configuration(), new Driver(), args);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		System.exit(code);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();
		
		//job to to group moveies by viewcount wise
		Job job = Job.getInstance(conf, "GroupMoviesByViews");

		job.setJarByClass(Driver.class);
		// job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ViewedMapper.class);
		job.setCombinerClass(ViewedReducer.class);
		job.setReducerClass(ViewedReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		job.waitForCompletion(true);
		
		//job to get top k viewed movies
		
		Job job2=Job.getInstance(conf,"TopViewed");
		job2.setJarByClass(Driver.class);
		
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapperClass(TopViewedMapper.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(TopViewedReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);
		//no of reduce task to be 1 because we want all data together to find top k viewed movies
		job2.setNumReduceTasks(1);
		job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		
		job2.addCacheFile(new URI("/movielens/movies.dat"));
		FileInputFormat.addInputPath(job2, new Path(arg0[1]));
		FileOutputFormat.setOutputPath(job2, new Path(arg0[2]));
		return job2.waitForCompletion(true)?0:1;
		
	}

}
