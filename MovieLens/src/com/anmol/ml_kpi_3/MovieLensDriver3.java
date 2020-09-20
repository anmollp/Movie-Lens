package com.anmol.ml_kpi_3;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MovieLensDriver3 {

public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
	Configuration conf = new Configuration();
	String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (pathArgs.length < 7)
    {
      System.err.println("Movie Lens KPI 2 Usage: MovieLensDriver2 <input-path> [...] <output-path>");
      System.exit(2);
    }


    FileUtils.deleteDirectory(new File(args[2]));

	
	conf.set("id.to.profession.mapping.file.path", args[5]);
	Job job1 = Job.getInstance(conf);
	job1.setJarByClass(MovieLensDriver3.class);
	job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
	TextOutputFormat.setOutputPath(job1, new Path(args[2]));
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(Text.class);
	MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingDataMapper.class);
	MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, UsersDataMapper.class);
	job1.setReducerClass(UsersRatingJoinReducer.class);
	int code = job1.waitForCompletion(true) ? 0 : 1;
	int code2 = 1;
	if (code == 0) {

			Job sampleJob2 = Job.getInstance(conf);
			sampleJob2.setJarByClass(MovieLensDriver3.class);
			sampleJob2.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
			TextOutputFormat.setOutputPath(sampleJob2, new Path(args[3]));
			sampleJob2.setOutputKeyClass(Text.class);
			sampleJob2.setOutputValueClass(Text.class);
			sampleJob2.setReducerClass(MoviesRatingJoinReducer.class);
			MultipleInputs.addInputPath(sampleJob2, new Path(args[2]), TextInputFormat.class,
					UsersRatingDataMapper.class);
			MultipleInputs.addInputPath(sampleJob2, new Path(args[4]), TextInputFormat.class, MoviesDataMapper.class);
			code2 = sampleJob2.waitForCompletion(true) ? 0 : 1;

	}

	if (code2 == 0) {
	
	Job job3 = Job.getInstance(conf);
	job3.setJarByClass(MovieLensDriver3.class);
	job3.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
	job3.setJobName("Find_Highest_Rank");
	FileInputFormat.addInputPath(job3, new Path(args[3]));
	FileOutputFormat.setOutputPath(job3, new Path(args[6]));
	job3.setMapperClass(AggregatedDataMapper.class);
	job3.setReducerClass(AggregatedReducer.class);
	job3.setPartitionerClass(AgeGroupPartitioner.class);
	job3.setNumReduceTasks(3);
	job3.setOutputKeyClass(Text.class);
	job3.setOutputValueClass(Text.class);
	System.exit(job3.waitForCompletion(true) ? 0 : 1);
	
	}

}
}
