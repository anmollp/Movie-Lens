package com.anmol.ml_kpi_2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MostRatedMapper1 extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("::");
		String movieId = values[1];
		String rating = values[2];
		context.write(new LongWritable(Long.parseLong(movieId)), new LongWritable(Long.parseLong(rating)));
	}

}
