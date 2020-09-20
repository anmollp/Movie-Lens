package com.anmol.ml_kpi_2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MostRatedMapper2 extends Mapper<LongWritable, Text, LongWritable, MovieRatingJoinWritable>{
	
	String inputFileName;
	
	public void setup(Context context) {
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		inputFileName = fileSplit.getPath().getName();
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("::");
		context.write(new LongWritable(Long.parseLong(values[0])), new MovieRatingJoinWritable(values[1], inputFileName));
	}

}
