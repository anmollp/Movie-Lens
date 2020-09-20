package com.anmol.ml_kpi_2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MostRatedReducer1 extends Reducer<LongWritable, LongWritable, NullWritable, Text> {

	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long userCount = 0;
		double ratingSum = 0;
		for(LongWritable value : values) {
			ratingSum += value.get();
			userCount += 1;
		}
		if(userCount >= 40) {
			long averageRating = Math.round(ratingSum/userCount);
			context.write(NullWritable.get(), new Text(key + "::" + averageRating));
		}
	}
}
