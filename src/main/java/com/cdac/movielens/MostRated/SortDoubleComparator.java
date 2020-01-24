package com.cdac.movielens.MostRated;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortDoubleComparator extends WritableComparator {

	public SortDoubleComparator() {
		super(DoubleWritable.class, true);
	}

	@Override

	public int compare(WritableComparable a, WritableComparable b) {
		DoubleWritable k1=(DoubleWritable)a;
		DoubleWritable k2=(DoubleWritable)b;
		return -super.compare(k1, k2);
	}

}
