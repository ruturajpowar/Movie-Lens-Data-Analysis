package com.cdac.movielens.genresRanking;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdJobAggregateDataMapper extends Mapper<Object, Text, Text, Text>{
	
	private Text outKey=new Text();
	private Text outValue=new Text();
	
	@Override
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		
		String[] field = value.toString().split("::");
		outKey.set(field[2]+"::"+field[1]);
		outValue.set(field[0]+"::"+field[3]);
		context.write(outKey, outValue);
	}

}
