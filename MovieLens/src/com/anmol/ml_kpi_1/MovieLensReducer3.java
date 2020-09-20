package com.anmol.ml_kpi_1;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieLensReducer3 extends Reducer<Text, LongWritable, NullWritable, Text>{
	
	private TreeMap<Long, String> tmap;
	private TreeMap<String, Integer> movieMap;
	
	public void setup(Context context) {
		tmap = new TreeMap<>();
		movieMap = new TreeMap<>();
	}

	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long viewCount = 0;
		for(LongWritable value: values) {
			viewCount += value.get();
		}
		tmap.put(viewCount, key.toString());
		
		if(tmap.size() > 10) {
			tmap.remove(tmap.firstKey());
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		
		for(Map.Entry<Long, String> entry : tmap.entrySet()) {
			movieMap.put(entry.getValue(), 1);
		}
		for(Map.Entry<String, Integer> entry : movieMap.entrySet()) {
			context.write(NullWritable.get(), new Text(entry.getKey()));
		}
	}
}
