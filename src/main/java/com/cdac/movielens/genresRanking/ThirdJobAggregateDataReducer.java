package com.cdac.movielens.genresRanking;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdJobAggregateDataReducer extends Reducer<Text, Text, Text, Text> {

	private Map<String, GenreRanking> map = new HashMap<String, GenreRanking>();
	private TreeMap<Double,String> finalMap=new TreeMap<Double, String>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text value : values) {
			String[] field = value.toString().split("::");

			String genre = field[0];
			double rating = Double.parseDouble(field[1]);

			GenreRanking genreRanking = map.get(genre);
			if (genreRanking != null) {
				genreRanking.setSum(genreRanking.getSum() + rating);
				genreRanking.setCount(genreRanking.getCount() + 1);
			} else {
				GenreRanking rankingNew = new GenreRanking();
				rankingNew.setSum(rating);
				rankingNew.setCount(1);
				map.put(genre, rankingNew);
			}
		}
		
		for (Map.Entry<String, GenreRanking> entry : map.entrySet()) {
			GenreRanking ranking = entry.getValue();
			double average=ranking.getSum()/ranking.getCount();
			finalMap.put(average, entry.getKey());
		}
		
		StringBuilder sb=new StringBuilder();
		int count=0;
		
		for(Map.Entry<Double, String>entry:finalMap.descendingMap().entrySet()) {
			if(count<5) {
				sb.append(entry.getValue()+"::");
				count++;
			}else {
				break;
			}
		}
		context.write(key, new Text(sb.toString().substring(0,sb.toString().lastIndexOf("::"))));
		
	}

}

class GenreRanking {

	private String genre;
	private double sum;
	private int count;

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

	public double getSum() {
		return sum;
	}

	public void setSum(double sum) {
		this.sum = sum;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

}
