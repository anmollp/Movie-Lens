package com.anmol.ml_kpi_2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MostRatedReducer3 extends Reducer<LongWritable, Text, NullWritable, Text> {
	
	private int ratings = 0;
	
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for(Text value : values) {
			if(ratings < 20) {
				context.write(NullWritable.get(), new Text(value));
				}
			else {
				break;
			}
			ratings++;
			}
	}

}
