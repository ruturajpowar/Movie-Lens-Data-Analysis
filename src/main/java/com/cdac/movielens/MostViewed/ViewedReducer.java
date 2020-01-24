package com.cdac.movielens.MostViewed;

import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ViewedReducer extends Reducer<Text,IntWritable,Text , IntWritable> {

	@Override
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int counter=StreamSupport.stream(values.spliterator(), true).mapToInt(IntWritable::get).sum();
		context.write(key, new IntWritable(counter));
	}
}
