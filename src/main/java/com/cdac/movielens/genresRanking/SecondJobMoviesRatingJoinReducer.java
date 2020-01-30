package com.cdac.movielens.genresRanking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondJobMoviesRatingJoinReducer extends Reducer<Text, Text, Text, Text> {

	private List<Text> moviesList=new ArrayList<Text>();
	private List<Text> ratingList=new ArrayList<Text>();
	private Text movieValue=new Text();
	private Text ratingValue=new Text();
	private Text outValue=new Text();
	
	@Override
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		moviesList.clear();
		ratingList.clear();
		for (Text value : values) {
			if(value.toString().startsWith("M")){
				movieValue.set(value.toString().substring(1));
				moviesList.add(movieValue);
			}else if(value.toString().startsWith("C")) {
				ratingValue.set(value.toString().substring(1));
				ratingList.add(ratingValue);
			}
		}
		
		
		if(!ratingList.isEmpty() && !moviesList.isEmpty()) {
			for (Text movie : moviesList) {
				String[] field = movie.toString().split("::");
				//genres are pipe seperated.
				// | is a metacharacter in regex. You'd need to escape it:
				String[] genres = field[1].split("\\|");
				
				//for each ratingData ie age grp and occupation we need to attatch genres
				for (Text ratingData : ratingList) {
					for (String genre : genres) {
						outValue.set(genre);
						context.write(outValue, ratingData);
					}
					
				}
			}
		}
	}
}
//Output sample from second job
//Animation::18-35::programmer::5