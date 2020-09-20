package com.anmol.ml_kpi_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MovieLensMapper2 extends Mapper<LongWritable, Text, IntWritable, MovieJoinWritable>{
	
	String inputFileName;
	
	@Override
	protected void setup(Context context) {
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		inputFileName = fileSplit.getPath().getName();
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values  = value.toString().split("::");
		context.write(new IntWritable(Integer.parseInt(values[0])), new MovieJoinWritable(values[1], inputFileName));
		
	}

}
