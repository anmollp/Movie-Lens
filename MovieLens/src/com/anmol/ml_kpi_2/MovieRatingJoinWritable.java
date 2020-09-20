package com.anmol.ml_kpi_2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MovieRatingJoinWritable implements Writable{
	private Text mrValue;
	private Text mrFileName;
	
	public MovieRatingJoinWritable() {
		set(new Text(), new Text());
	}
	
	public MovieRatingJoinWritable(String mrValue, String mrFileName) {
		set(new Text(mrValue), new Text(mrFileName));
	}
	
	public MovieRatingJoinWritable(Text mrValue, Text mrFileName) {
		set(mrValue, mrFileName);
	}

	private void set(Text mrValue, Text mrFileName) {
		this.mrValue = mrValue;
		this.mrFileName = mrFileName;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mrValue.readFields(in);
		mrFileName.readFields(in);	
	}

	@Override
	public void write(DataOutput out) throws IOException {
		mrValue.write(out);
		mrFileName.write(out);
	}
	
	@Override
	public int hashCode()
	{
		return mrValue.hashCode() * 163 + mrFileName.hashCode();
	}
	
	public String toString()
	{
		return mrValue + "\t" + mrFileName;
	}

	public Text getMrValue() {
		return mrValue;
	}
	
	public Text getMrFileName() {
		return mrFileName;
	}
}
