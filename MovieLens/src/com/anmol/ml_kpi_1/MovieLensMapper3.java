package com.anmol.ml_kpi_1;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieLensMapper3 extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	private TreeMap<Long, String> tmap;
	
	public void setup(Context context) {
		tmap = new TreeMap<>();
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("::");
		tmap.put(Long.parseLong(values[1]), values[0]);
		
		if (tmap.size() > 10 ) {
			tmap.remove(tmap.firstKey());
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		for(Map.Entry<Long, String> entry : tmap.entrySet()) {
			context.write(new Text(entry.getValue()), new LongWritable(entry.getKey())); 
		}
	}

}
