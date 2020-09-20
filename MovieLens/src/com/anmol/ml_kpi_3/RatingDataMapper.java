package com.anmol.ml_kpi_3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingDataMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("::");
		
		String userId = values[0];
		String movieId = values[1];
		String rating = values[2];
		
		context.write(new Text(userId), new Text("rating" + movieId + "::" + rating));
	}

}
