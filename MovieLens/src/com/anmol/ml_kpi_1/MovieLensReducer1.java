package com.anmol.ml_kpi_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieLensReducer1 extends Reducer<Text, IntWritable, NullWritable, Text> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		for(IntWritable value: values) {
			count += value.get();
		}
		context.write(NullWritable.get(), new Text(key.toString()+"::"+count));
	}

}
