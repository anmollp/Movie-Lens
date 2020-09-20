package com.anmol.ml_kpi_3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class AggregatedDataMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text keyData = new Text();
	private Text outvalue = new Text();
	
	@Override
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		String data = values.toString();
		String[] field = data.split("::");
		if (null != field && field.length == 4) {
			keyData.set(field[2] + "::" + field[1]);
			outvalue.set(field[0] + "::" + field[3]);
			context.write(keyData, outvalue);
		}
	}
}


