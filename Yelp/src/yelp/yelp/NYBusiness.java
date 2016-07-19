package yelp.yelp;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NYBusiness
{
	/* Mapper */
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			//from business
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(),delims);
			
			if (businessData.length == 3)
			{
				String id = businessData[0];
				String addr = businessData[1];
				String id_addr = id + "^" + addr;
				if(addr.contains("NY"))
					context.write(new Text(id_addr), new IntWritable(1));
			}		
		}		
	}

	/* Reducer */
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>
	{		
		@SuppressWarnings("unused")
		public void reduce(Text key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException
		{		
			int count=0;
			
			for(IntWritable t : values)
			{
				count++;
			}			
			context.write(new Text(key),new IntWritable(count));			
		}
	}	
	
	// Driver program
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();// get all args
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: CountYelpBusiness <in> <out>");
			System.exit(2);
		}
			  
		Job job = Job.getInstance(conf, "CountYelp");
		job.setJarByClass(YelpBusinessCount.class);
	   
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		// set output key type		
		job.setOutputKeyClass(Text.class);		
		
		// set output value type
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);		
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
