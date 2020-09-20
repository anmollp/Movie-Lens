package com.anmol.ml_kpi_3;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UsersRatingJoinReducer extends Reducer<Text, Text, Text, Text>{
	
	private ArrayList<Text> listUsers = new ArrayList<Text>();
	private ArrayList<Text> listRating = new ArrayList<Text>();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		listUsers.clear();
		listRating.clear();
		
		for (Text text : values) {
			if (text.toString().startsWith("user")) {
				listUsers.add(new Text(text.toString().substring(4)));
			} else if (text.toString().startsWith("rating")) {
				listRating.add(new Text(text.toString().substring(6)));
			}
	}
		executeJoinLogic(context);
	}
	
	private void executeJoinLogic(Context context) throws IOException, InterruptedException {
		if (!listUsers.isEmpty() && !listRating.isEmpty()) {
		for (Text usersData : listUsers) {
			for (Text ratingData : listRating) {
				context.write(usersData, ratingData);
				}
			}
			}
		}

}
