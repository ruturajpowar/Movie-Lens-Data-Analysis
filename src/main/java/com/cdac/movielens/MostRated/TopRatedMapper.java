package com.cdac.movielens.MostRated;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopRatedMapper extends Mapper<Text, Text, DoubleWritable, Text> {

	@Override
	public void map(Text movieId,Text avgRating,Context context) throws IOException, InterruptedException {
		context.write( new DoubleWritable(Double.parseDouble(avgRating.toString())),movieId);
	}
}
