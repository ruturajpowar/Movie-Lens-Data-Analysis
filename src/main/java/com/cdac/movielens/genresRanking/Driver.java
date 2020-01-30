package com.cdac.movielens.genresRanking;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Driver extends Configured implements Tool {

	public static void main(String[] args) {
		int code = -1;
		if (args.length != 6) {
			System.err.println("Please specify the input and output path");
			System.exit(code);
		}
		try {
			code = ToolRunner.run(new Configuration(), new Driver(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(code);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		// Configuration for first job
		Job firstJob = Job.getInstance(conf);

		firstJob.setJarByClass(this.getClass());
		firstJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");

		MultipleInputs.addInputPath(firstJob, new Path(args[0]), TextInputFormat.class, FirstJobUsersMapper.class);
		MultipleInputs.addInputPath(firstJob, new Path(args[1]), TextInputFormat.class, FirstJobRatingsMapper.class);

		firstJob.setReducerClass(FirstJobReducer.class);
		firstJob.setOutputKeyClass(Text.class);
		firstJob.setOutputValueClass(Text.class);
		TextOutputFormat.setOutputPath(firstJob, new Path(args[2]));
		int code = firstJob.waitForCompletion(true) ? 0 : 1;
		int code2=1;
		if(code ==0) {
			// Configuration for second job
			Job secondJob=Job.getInstance(conf);
			secondJob.setJarByClass(this.getClass());
			secondJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
			
			MultipleInputs.addInputPath(secondJob, new Path(args[2]), TextInputFormat.class, SecondJobUsersRatingDataMapper.class);
			MultipleInputs.addInputPath(secondJob, new Path(args[3]), TextInputFormat.class, SecongJobMoviesDataMapper.class);
			
			secondJob.setReducerClass(SecondJobMoviesRatingJoinReducer.class);
			secondJob.setOutputKeyClass(Text.class);
			secondJob.setOutputValueClass(Text.class);
			
			TextOutputFormat.setOutputPath(secondJob, new Path(args[4]));
			code2=secondJob.waitForCompletion(true)?0:1;
			
		}
		if(code2==0) {
			Job thirdJob=Job.getInstance(conf);
			thirdJob.setJarByClass(this.getClass());
			thirdJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
			
			thirdJob.setMapperClass(ThirdJobAggregateDataMapper.class);
			thirdJob.setReducerClass(ThirdJobAggregateDataReducer.class);
			thirdJob.setNumReduceTasks(1);
			thirdJob.setOutputKeyClass(Text.class);
			thirdJob.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(thirdJob, new Path(args[4]));
			FileOutputFormat.setOutputPath(thirdJob, new Path(args[5]));
			System.exit(thirdJob.waitForCompletion(true)?0:1);
		}
		return code;
	}

}
