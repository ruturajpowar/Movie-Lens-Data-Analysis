package com.cdac.movielens.MostRated;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.OptionalDouble;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text movieId, Iterable<IntWritable> ratings, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		double sum = 0;
		Iterator<IntWritable> itr = ratings.iterator();
		while (itr.hasNext()) {
			sum = sum + itr.next().get();
			count++;
		}
		if (count > 40) {
			context.write(movieId, new DoubleWritable(sum / count));
		}

	}
}
