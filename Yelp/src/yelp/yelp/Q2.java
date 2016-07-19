package yelpjoin.yelpjoin;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q2 extends Configured implements Tool
{
	/* Mapper */	
	public static class GenMap extends Mapper<LongWritable, Text, TupleI, Text>
	{
		public static String uName = "";
		
		 protected void setup(Context context) throws IOException
		 {
			Configuration conf = context.getConfiguration();
			uName = conf.get("userName");			
		 }
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			
			if(fileName.startsWith("user"))
			{
				String delims = "^";
				String[] userData = StringUtils.split(value.toString(),delims);				
				
				if(userData[1].equalsIgnoreCase(uName))
				{
					//userId -> userName
					context.write(new TupleI(userData[0].trim(), 0), new Text(userData[1]));					
				}
			}
			else if(fileName.startsWith("review"))
			{
				String delims = "^";
				String[] reviewData = StringUtils.split(value.toString(),delims);				
				//businessId -> rating
				context.write(new TupleI(reviewData[1], 1), new Text(reviewData[3]));
			}				
		}		
	}
	
	public class Q2Partitioner extends Partitioner<TupleI, Text> 
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
		HashMap<String, HashSet<String>> rMap = new HashMap<String, HashSet<String>>();		
		double sum = 0;
		int count = 0;		
		
		public void reduce(TupleI key, Iterable<Text> values,Context context ) throws IOException, InterruptedException
		{
			//review table
			if(key.getId() == 1)
			{
				if(rMap.containsKey(key.getStr()))
				{
					for(Text t : values)
					{					
						sum += Double.parseDouble(t.toString());
						count++;
					}				
				
					for(String s : rMap.get(key.getStr()))
						context.write(new Text(s), new DoubleWritable(sum / count));
				
					rMap.clear();				
					count = 0;
					sum = 0;
				}
			}
			else
			{
				HashSet<String> temp = new HashSet<String>();
				for(Text t : values)
				{
					temp.add(t.toString());									
				}
				rMap.put(key.getStr(), temp);
			}					
		}		
	}
	
	// Driver program
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new Q2(), args);
	    System.exit(res);
		//hadoop jar YelpJoin-0.0.1-SNAPSHOT.jar yelpjoin.yelpjoin.Q2 -D userName="Leesha Z." /user/ass150430/Yelp_Q2 /user/ass150430/Q2_OP
	}

	public int run(String[] args) throws Exception 
	{
		Configuration conf = this.getConf();
		
		if (args.length < 2)
		{
			System.err.println("Usage: Q2 <in> <out>");
			System.exit(2);
		}		
			  
		Job job = Job.getInstance(conf, "YelpJoin");
		job.setJarByClass(Q2.class);
	   
		job.setMapperClass(GenMap.class);
		job.setReducerClass(Reduce.class);
		
		// set output key type		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		// set output value type
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(TupleI.class);		
		
		job.setPartitionerClass(Q2Partitioner.class);        
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		
		//Wait till job completion
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
