package com.cdac.movielens.genresRanking;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstJobRatingsMapper extends Mapper<Object, Text, Text, Text>{
	
	private Text outKey = new Text();
	private Text outValue = new Text();
	@Override
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		String[] ratingsFields = value.toString().split("::");
		if(ratingsFields !=null && ratingsFields.length==4 && ratingsFields[0].length()>0) {
		outKey.set(ratingsFields[0]);
		outValue.set("R"+ratingsFields[1]+"::"+ratingsFields[2]);
		context.write(outKey, outValue);
		}
	}
}
