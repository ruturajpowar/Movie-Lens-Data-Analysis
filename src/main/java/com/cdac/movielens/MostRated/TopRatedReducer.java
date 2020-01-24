package com.cdac.movielens.MostRated;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopRatedReducer extends Reducer<DoubleWritable,Text, Text, DoubleWritable>{
	private int counter=0;
	private int topk=10;
	
	private Map<String,String> moviesMap=new HashMap<String, String>();
	
	@Override
	protected void setup(Context context) throws IOException {
		topk=context.getConfiguration().getInt("topk", topk);
		URI[] cacheFiles = context.getCacheFiles();
		if(cacheFiles !=null && cacheFiles.length>0) {
			
			FileSystem fs=FileSystem.get(context.getConfiguration());
			Path path=new Path(cacheFiles[0].toString());
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
			StreamSupport.stream(reader.lines().spliterator(), true).forEach(line->{
				moviesMap.put(line.split(":")[0], line.split("::")[1]);
			});
			reader.close();			
		
		}
	}
	
	@Override
	public void reduce(DoubleWritable avgRating,Iterable<Text> movieIds,Context context ) {
		if(counter < topk) {
			movieIds.forEach(movieId->{
				try {
					context.write(new Text(moviesMap.get(movieId.toString())), avgRating);
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			counter++;
		}
		
	}
}
