package com.cdac.movielens.genresRanking;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecongJobMoviesDataMapper extends Mapper<Object, Text, Text, Text>{
	
	private Text movieId=new Text();
	private Text outValue=new Text();
	
	@Override
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		String[] field = value.toString().split("::");
		if(null !=field && field.length ==3 && field[0].length()>0) {
			movieId.set(field[0]);
			outValue.set("M"+field[1]+"::"+field[2]);
			context.write(movieId, outValue);
		}
	}

}
