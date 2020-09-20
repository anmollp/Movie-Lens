package com.anmol.ml_kpi_3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AgeGroupPartitioner extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		int partitionNo = 0;
		String ageGroup = key.toString().split("::")[1];
		
		if(numReduceTasks != 0) {
			if (ageGroup.equals("18-35")) {
				partitionNo = 0;
			}
			else if (ageGroup.equals("36-50")) {
				partitionNo = 1;
			}
			else if(ageGroup.equals("50+")) {
				partitionNo = 2;
			}
		}
		return partitionNo;
	}

}
