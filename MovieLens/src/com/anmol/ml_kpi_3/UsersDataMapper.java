package com.anmol.ml_kpi_3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UsersDataMapper extends Mapper<LongWritable, Text, Text, Text>{

	private HashMap<Integer, String> profession_mapping = new HashMap<Integer, String>();
	private String profession_id_file_location = null;
	
	public void setup(Context context) {
		profession_id_file_location = context.getConfiguration().get("id.to.profession.mapping.file.path");
		try {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(profession_id_file_location));
		String line = null;
		while ((line = bufferedReader.readLine()) != null) {
			String[] field = line.split(",");
			profession_mapping.put(Integer.parseInt(field[0]), field[1]);
			System.out.println(profession_mapping.get(Integer.parseInt(field[0])));
			}
		bufferedReader.close();
		} catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
			}
		}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] values = value.toString().split("::");
		
		String userId = values[0];
		String profession = profession_mapping.get(Integer.parseInt(values[3]));
		String ageRange = AgeStringFactory.getAgeString(values[2]);
		if (ageRange != null) {
			context.write(new Text(userId), new Text("user"+ ageRange + "::" + profession));
		}
	}
}
