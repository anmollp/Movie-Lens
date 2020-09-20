package com.anmol.ml_kpi_2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MovieLensDriver2 {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (pathArgs.length < 3)
	    {
	      System.err.println("Movie Lens KPI 2 Usage: MovieLensDriver2 <input-path> [...] <output-path>");
	      System.exit(2);
	    }
	    
	    Job mrm = Job.getInstance(conf, "Top rated movies part 1");
		mrm.setJarByClass(MovieLensDriver2.class);
		mrm.setMapperClass(MostRatedMapper1.class);
		mrm.setReducerClass(MostRatedReducer1.class);
		
		mrm.setMapOutputKeyClass(LongWritable.class);
		mrm.setMapOutputValueClass(LongWritable.class);
		mrm.setOutputKeyClass(NullWritable.class);
		mrm.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(mrm, new Path(pathArgs[0]));
		FileOutputFormat.setOutputPath(mrm, new Path("/movie-lens/ratings-intermediate-output-1"));
		mrm.waitForCompletion(true);
		
		Job mrm2 = Job.getInstance(conf, "Top rated movies part 2");
		mrm2.setJarByClass(MovieLensDriver2.class);
		mrm2.setMapperClass(MostRatedMapper2.class);
		mrm2.setReducerClass(MostRatedReducer2.class);
		
		mrm2.setMapOutputKeyClass(LongWritable.class);
		mrm2.setMapOutputValueClass(MovieRatingJoinWritable.class);
		mrm2.setOutputKeyClass(NullWritable.class);
		mrm2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(mrm2, new Path("/movie-lens/ratings-intermediate-output-1"));
		FileInputFormat.addInputPath(mrm2, new Path(pathArgs[1]));
		FileOutputFormat.setOutputPath(mrm2, new Path("/movie-lens/ratings-intermediate-output-2"));
		mrm2.waitForCompletion(true);
		
		Job mrm3 = Job.getInstance(conf, "Top rated movies part 3");
		mrm3.setJarByClass(MovieLensDriver2.class);
		mrm3.setMapperClass(MostRatedMapper3.class);
		mrm3.setReducerClass(MostRatedReducer3.class);
		mrm3.setNumReduceTasks(1);
		
		mrm3.setMapOutputKeyClass(LongWritable.class);
		mrm3.setMapOutputValueClass(Text.class);
		mrm3.setOutputKeyClass(NullWritable.class);
		mrm3.setOutputValueClass(Text.class);
		
		
		mrm3.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		
		FileInputFormat.addInputPath(mrm3, new Path("/movie-lens/ratings-intermediate-output-2"));
		FileOutputFormat.setOutputPath(mrm3, new Path(pathArgs[2]));
		System.exit(mrm3.waitForCompletion(true) ? 0 : 1);
	}

}
