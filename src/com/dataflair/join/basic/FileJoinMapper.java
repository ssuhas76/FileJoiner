package com.dataflair.join.basic;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.examples.Join;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileJoinMapper extends Mapper<LongWritable, Text, Text, JoinWritable>{
	
	String inputFileName;

	@Override
	protected void setup(Context context) throws IOException,InterruptedException
	{
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		inputFileName = fileSplit.getPath().getName();
	}
	
	public void map(LongWritable key, Text values, Context context) {
		String [] tokens = values.toString().split(",");
		
		if (tokens.length == 2) {
			try {
				context.write(new Text(tokens[0]), new JoinWritable(tokens[1],inputFileName));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
