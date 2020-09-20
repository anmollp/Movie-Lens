package com.anmol.ml_kpi_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieLensReducer2 extends Reducer<IntWritable, MovieJoinWritable, NullWritable, Text>{
	private String name = null;
	private int count;
	public void reduce(IntWritable key, Iterable<MovieJoinWritable> values, Context context) throws IOException, InterruptedException {
		for(MovieJoinWritable value: values) {
			if (value.getMrFileName().toString().equals("movies.dat")) {
				name = value.getMrValue().toString();
			}
			else {
				count = Integer.parseInt(value.getMrValue().toString());
			}
		}
		context.write(NullWritable.get(), new Text(name + "::" + count));
	}

}
