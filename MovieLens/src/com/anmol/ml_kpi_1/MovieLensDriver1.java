package com.anmol.ml_kpi_1;

import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class MovieLensDriver1 {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (pathArgs.length < 3)
	    {
	      System.err.println("Movie Lens KPI 1 Usage: MovieLensDriver1 <input-path> [...] <output-path>");
	      System.exit(2);
	    }
		Job mvm = Job.getInstance(conf, "Top viewed movies part 1");
		mvm.setJarByClass(MovieLensDriver1.class);
		mvm.setMapperClass(MovieLensMapper1.class);
		mvm.setReducerClass(MovieLensReducer1.class);
		
		mvm.setMapOutputKeyClass(Text.class);
		mvm.setMapOutputValueClass(IntWritable.class);
		mvm.setOutputKeyClass(NullWritable.class);
		mvm.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(mvm, new Path(pathArgs[0]));
		
//		FileInputFormat.addInputPath(mvm, new Path("/home/anmol/practice/Movies-Data/ratings.dat"));
		FileOutputFormat.setOutputPath(mvm, new Path("/movie-lens/intermediate-output-1"));
		mvm.waitForCompletion(true);
		
		Job mvm2 = Job.getInstance(conf, "Top viewed movies part 2");
		mvm2.setJarByClass(MovieLensDriver1.class);
		mvm2.setMapperClass(MovieLensMapper2.class);
		mvm2.setReducerClass(MovieLensReducer2.class);
		
		mvm2.setMapOutputKeyClass(IntWritable.class);
		mvm2.setMapOutputValueClass(MovieJoinWritable.class);
		mvm2.setOutputKeyClass(NullWritable.class);
		mvm2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(mvm2, new Path(pathArgs[1]));
		FileInputFormat.addInputPath(mvm2, new Path("/movie-lens/intermediate-output-1"));
		FileOutputFormat.setOutputPath(mvm2, new Path("/movie-lens/intermediate-output-2"));
		
		mvm2.waitForCompletion(true);
		
		Job mvm3 = Job.getInstance(conf, "Top viewed movies part 3");
		mvm3.setJarByClass(MovieLensDriver1.class);
		mvm3.setMapperClass(MovieLensMapper3.class);
		mvm3.setReducerClass(MovieLensReducer3.class);
		
		mvm3.setMapOutputKeyClass(Text.class);
		mvm3.setMapOutputValueClass(LongWritable.class);
		mvm3.setOutputKeyClass(NullWritable.class);
		mvm3.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(mvm3, new Path("/movie-lens/intermediate-output-2"));
		FileOutputFormat.setOutputPath(mvm3, new Path(pathArgs[2]));
		
		System.exit(mvm3.waitForCompletion(true) ? 0 : 1);
	}
	

}
