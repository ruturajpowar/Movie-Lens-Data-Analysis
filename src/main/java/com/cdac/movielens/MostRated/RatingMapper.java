package com.cdac.movielens.MostRated;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		//output wiil be movieId->rating
		context.write(new Text(value.toString().split("::")[1]),new IntWritable(Integer.parseInt(value.toString().split("::")[2])));
	}
}
