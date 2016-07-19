package yelp.yelp;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Q3
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
				
				Pattern p = Pattern.compile("(stanford,)");
				Matcher m = p.matcher(businessData[1].toLowerCase());
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
				StringBuilder val = new StringBuilder();
				val.append(reviewData[1]).append("\t").append(reviewData[3]);
				//businessId -> userId + rating
				context.write(new TupleI(reviewData[2], 1), new Text(val.toString()));
			}				
		}		
	}
	
	public class Q3Partitioner extends Partitioner<TupleI, Text> 
	{
		@Override
		public int getPartition(TupleI key, Text value, int numPartitions)
		{
			String word = key.getStr();			
			return (word.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	/* Reducer */
	public static class Reduce extends Reducer<TupleI,Text,Text,DoubleWritable>
	{		
		String prevBid = "";
		
		public void reduce(TupleI key, Iterable<Text> values,Context context ) throws IOException, InterruptedException
		{			
			//review table
			if(key.getId() == 1)
			{
				if(prevBid != null && prevBid.equals(key.getStr()))
				{					
					for(Text t : values)					
					{
						String[] val = t.toString().split("\t");
						context.write(new Text(val[0]), new DoubleWritable(Double.parseDouble(val[1])));						
					}
				}
				prevBid = "";
			}
			else if(key.getId() == 0)
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
		job.setJarByClass(Q3.class);
	   
		job.setMapperClass(GenMap.class);
		job.setReducerClass(Reduce.class);
		
		// set output key type		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		// set output value type
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(TupleI.class);	
		
		job.setPartitionerClass(Q3Partitioner.class);
				
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		//hadoop jar Yelp-0.0.1-SNAPSHOT.jar yelp.yelp.Q3 /user/ass150430/Yelp_Q3 /user/ass150430/Q3_OP
	}
}
