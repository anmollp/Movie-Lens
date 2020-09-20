package com.anmol.ml_kpi_1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MovieJoinWritable implements WritableComparable<MovieJoinWritable>{
	private Text mrValue;
	private Text mrFileName;
	
	public MovieJoinWritable()
	{
		set(new Text(), new Text());
	}
	
	public MovieJoinWritable(Text mrValue, Text mrFileName) {
		set(mrValue, mrFileName);
	}
	
	public MovieJoinWritable(String mrValue, String mrFileName)
	{
		set(new Text(mrValue), new Text(mrFileName));
	}
	
	private void set(Text mrValue, Text mrFileName) {
		this.mrValue = mrValue;
		this.mrFileName = mrFileName;	
	}

	public Text getMrValue()
	{
		return mrValue;
	}

	public Text getMrFileName()
	{
		return mrFileName;
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

	@Override
	public int compareTo(MovieJoinWritable o) {
		return this.mrValue.toString().compareTo(o.mrValue.toString());
	}

}
