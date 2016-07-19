package yelp.yelp;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q5
{
	/* Mapper */
	public static class GenMap extends Mapper<LongWritable, Text, TupleI, Text>
	{		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			
			if(fileName.startsWith("business"))
			{
				String delims = "^";
				String[] businessData = StringUtils.split(value.toString(),delims);
				Pattern p = Pattern.compile("(\\w)?,?\\s+(TX)\\s+\\d{5}");
				Matcher m = p.matcher(businessData[1]);
				if(m.find())
				{
					//businessId -> empty string
					context.write(new TupleI(businessData[0], 0), new Text(""));
				}
			}
			else if(fileName.startsWith("review"))
			{
				String delims = "^";
				String[] reviewData = StringUtils.split(value.toString(),delims);				
				//businessId -> count
				context.write(new TupleI(reviewData[2], 1), new Text("1"));
			}				
		}		
	}
	
	public class Q5Partitioner extends Partitioner<TupleI, Text> 
	{
		@Override
		public int getPartition(TupleI key, Text value, int numPartitions)
		{
			String word = key.getStr();			
			return (word.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	/* Reducer */
	public static class Reduce extends Reducer<TupleI,Text,Text,IntWritable>
	{		
		String prevBid = "";		
		int sum = 0;		
		
		public void reduce(TupleI key, Iterable<Text> values,Context context ) throws IOException, InterruptedException
		{
			//review table
			if(key.getId() == 1)
			{
				if(prevBid != null && prevBid.equals(key.getStr()))
				{
					for(Text t : values)
					{					
						sum += Integer.parseInt(t.toString());					
					}
					context.write(new Text(key.getStr()), new IntWritable(sum));
					prevBid ="";
					sum = 0;
				}
			}
			else/* business table */
			{				
				prevBid = key.getStr();
			}			
		}		
	}	
	
	// Driver program
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();// get all args
		if (otherArgs.length < 2)
		{
			System.err.println("Usage: Q1 <in> <out>");
			System.exit(2);
		}
			  
		Job job = Job.getInstance(conf, "YelpJoin");
		job.setJarByClass(Q5.class);
	   
		job.setMapperClass(GenMap.class);
		job.setReducerClass(Reduce.class);
		
		// set output key type		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// set output value type
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(TupleI.class);	
		
		job.setPartitionerClass(Q5Partitioner.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		//hadoop jar Yelp-0.0.1-SNAPSHOT.jar yelp.yelp.Q5 /user/ass150430/Yelp_Q1 /user/ass150430/Q5_OP
	}
}
