package com.cdac.movielens.MostViewed;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopViewedMapper extends Mapper<Text, Text, LongWritable,Text > {
	
	@Override
	public void map(Text movieId,Text viewCount,Context context) throws IOException, InterruptedException  {
		
		System.out.println(viewCount);
		context.write(new LongWritable(Long.parseLong(viewCount.toString().trim())),movieId);
		
		
	}

}
