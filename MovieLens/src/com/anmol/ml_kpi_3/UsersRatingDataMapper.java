package com.anmol.ml_kpi_3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UsersRatingDataMapper extends Mapper<LongWritable, Text, Text, Text> {

private Text movieId = new Text();
private Text outvalue = new Text();

@Override
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String data = value.toString();
	String[] values = data.split("::");
	if (null != values && values.length == 4) {
		movieId.set(values[2]);
		outvalue.set("C" + values[0] + "::" + values[1]+"::"+values[3]);
		context.write(movieId, outvalue);

		}
	}

}
