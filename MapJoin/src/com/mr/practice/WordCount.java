package com.mr.practice;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job wordCountJob = Job.getInstance(conf, "Word Count");
		wordCountJob.setJarByClass(com.mr.practice.WordCount.class);
		wordCountJob.setMapperClass(wordCountMapper.class);
		wordCountJob.setReducerClass(wordCountReducer.class);
		
		//wordCountJob.setCombinerClass(wordCountReducer.class);
		//wordCountJob.setPartitionerClass(wordCountPartitioner.class);
		//wordCountJob.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));
		
	    wordCountJob.setOutputKeyClass(Text.class);
	    wordCountJob.setOutputValueClass(IntWritable.class);
		
		System.exit(wordCountJob.waitForCompletion(true)?0:1);
	}
	public static class  wordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		static IntWritable ONE = new IntWritable(1);
		public void map(LongWritable key, Text line, Context con) throws IOException, InterruptedException{
			StringTokenizer st = new StringTokenizer(line.toString());
			while(st.hasMoreTokens()){
				con.write(new Text(st.nextToken()), ONE);
			}
		}
	}
	public static class  wordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void recudce(Text word, Iterable<IntWritable> counts, Context con) throws IOException,InterruptedException{
		int totalCount = 0;
		for(IntWritable cnt:counts){
			totalCount +=cnt.get();
		}
		con.write(word, new IntWritable(totalCount));
		}
		
	}
	public static class wordCountPartitioner extends Partitioner<Text, IntWritable>{
		
		public int getPartition(Text word, IntWritable count, int noOfReducers ){
			char startChar = word.toString().charAt(0);
			if(startChar>=65 && startChar <=90){
				return 0;
			}else if (startChar>=97 && startChar <=122){
				return 1;
			}
			return 2;
		}
		
	}
}
