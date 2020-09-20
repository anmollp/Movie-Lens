package com.anmol.ml_kpi_2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MostRatedMapper3 extends Mapper<LongWritable, Text, LongWritable, Text>{

	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("::");
		context.write(new LongWritable(Long.parseLong(values[1])), new Text(values[0]));
	}

}
