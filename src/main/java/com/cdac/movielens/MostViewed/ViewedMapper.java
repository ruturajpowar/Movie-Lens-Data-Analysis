package com.cdac.movielens.MostViewed;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ViewedMapper extends Mapper<Object, Text, Text, IntWritable>{
	
	private static final IntWritable ONE=new IntWritable(1);
	 private Text word = new Text();
	
	@Override
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		word.set(value.toString().split("::")[1]);
		context.write(word, ONE);
	}
}
