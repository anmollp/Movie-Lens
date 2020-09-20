package com.anmol.ml_kpi_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieLensMapper1 extends Mapper<LongWritable, Text, Text, IntWritable >{
	Text movieId = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("::");
		movieId.set(values[1]);
		context.write(movieId, new IntWritable(1));
	}

}
