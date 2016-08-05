package com.mr.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MapJoin {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(MapJoin.class);
		job.setMapperClass(MapJoinMapper.class);
		job.setReducerClass(MapJoinReducer.class);
		
		
		
		
		
		
	}
	
	public static class MapJoinMapper extends Mapper<IntWritable, Text, Text, IntWritable> {
		
	}
	
	public static class MapJoinReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
	}

}
