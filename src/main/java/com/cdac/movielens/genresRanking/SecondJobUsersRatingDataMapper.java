package com.cdac.movielens.genresRanking;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondJobUsersRatingDataMapper extends Mapper<Object, Text, Text, Text> {
	private Text moveiId=new Text();
	private Text outValue=new Text();
	
	@Override
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		String[] data=value.toString().split("::");
		moveiId.set(data[2]);
		outValue.set("C"+data[0]+"::"+data[1]+"::"+data[3]);
		context.write(moveiId, outValue);
	}

}
