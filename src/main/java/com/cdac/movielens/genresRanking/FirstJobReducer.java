package com.cdac.movielens.genresRanking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FirstJobReducer extends Reducer<Text, Text, Text, Text>{
	
	private List<Text> listUsers=new ArrayList<Text>();
	private List<Text> listRating=new ArrayList<Text>();
	private Text userData=new Text();
	private Text ratingData=new Text();
	
	@Override
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		listUsers.clear();
		listRating.clear();
		
		for (Text value : values) {
			if(value.toString().startsWith("U")) {
				userData.set(value.toString().substring(1));
				listUsers.add(userData);
			}else if(value.toString().startsWith("R")) {
				ratingData.set(value.toString().substring(1));
				listRating.add(ratingData);
			}
			
		}
		
		if(!listRating.isEmpty() && !listUsers.isEmpty()) {
			for (Text userDataOut : listUsers) {
				for (Text ratingDataOut : listRating) {
					context.write(userDataOut, ratingDataOut);
				}
				
			}
		}
	}
}
