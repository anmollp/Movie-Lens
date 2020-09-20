package com.anmol.ml_kpi_2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MostRatedReducer2 extends Reducer<LongWritable, MovieRatingJoinWritable, NullWritable, Text>{

	String movieName = null;
	String rating = null;
	public void reduce(LongWritable key, Iterable<MovieRatingJoinWritable> values, Context context) throws IOException, InterruptedException {
		for(MovieRatingJoinWritable value : values) {
			if(value.getMrFileName().toString().equals("movies.dat")) {
				movieName = value.getMrValue().toString();
			}
			else {
				rating = value.getMrValue().toString();
			}
		}
		
		context.write(NullWritable.get(), new Text(movieName + "::" + rating));
	}
}
