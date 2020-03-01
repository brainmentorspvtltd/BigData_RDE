import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCount {

//	Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	
	public static class Map extends Mapper<LongWritable, Text,
	Text, IntWritable>{
			public void map(LongWritable key, Text value, Context context) 
					throws IOException, InterruptedException {
				String line = value.toString();
				StringTokenizer token = new StringTokenizer(line);
				while(token.hasMoreTokens()) {
					value.set(token.nextToken());
					context.write(value,  new IntWritable(1));
				}				
			}
	}
	
//	Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	
	public static class Reduce extends Reducer<Text, IntWritable,
	Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable x : values) {
				sum += x.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	
	public static void main(String[] args) throws IOException, 
	ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WordCount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);		
		Path outputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}

}



