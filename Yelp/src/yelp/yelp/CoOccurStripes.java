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

public class CoOccurStripes
{
	/* Mapper */
	public static class CoOccurStripeMap extends Mapper<LongWritable, Text, Text, MapWritable>
	{		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			//from business
			if(!value.toString().isEmpty())
			{
				String tokens = "[_|%$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
				String tok = "\\s+|\\d+";
				String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
				cleanLine = cleanLine.replaceAll(tok, " ");
				String[] words = StringUtils.split(cleanLine.toString()," ");			
				for(int i=0;i<words.length;++i)
				{
					if(!words[i].isEmpty())
					{
						MapWritable vector = new MapWritable();
						for(int j=i+1;j<words.length;++j)
						{
							if(vector.containsKey(words[j]))														
								vector.put(new Text(words[j]), new IntWritable(((IntWritable)vector.get(words[j])).get() + 1));
							else
								vector.put(new Text(words[j]), new IntWritable(1));						
						}
						if(!vector.isEmpty())
							context.write(new Text(words[i]), vector);
					}
				}
			}
		}		
	}

	/* Reducer */
	public static class Reduce extends Reducer<Text,MapWritable,Text,Text>
	{		
		public void reduce(Text key, Iterable<MapWritable> values,Context context ) throws IOException, InterruptedException
		{		
			MapWritable result = new MapWritable();
			
			for(MapWritable t : values)
			{
				for(Writable k : t.keySet())
				{
					if(result.containsKey((Text)k))
					{
						result.put((Text)k, new IntWritable(((IntWritable)result.get((Text)k)).get() + 1));
					}
					else
					{
						result.put((Text)k, new IntWritable(1));
					}							
				}
			}
			StringBuilder str = new StringBuilder();
			str.append("{");
			for(Writable k : result.keySet())
			{
				str.append(k.toString() + ":" + ((IntWritable)result.get((Text)k)).get() + ", ");
			}
			str.append("}");
			context.write(new Text(key + "------>"),new Text(str.toString()));			
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
		job.setJarByClass(CoOccurStripes.class);
	   
		job.setMapperClass(CoOccurStripeMap.class);
		job.setReducerClass(Reduce.class);
		
		// set output key type
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);		
		
		// set output value type
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputValueClass(Text.class);		
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		// hadoop jar Yelp-0.0.1-SNAPSHOT.jar yelp.yelp.CoOccurStripes /user/ass150430/WordCoOccurInput /user/ass150430/WordCoOccurOutput
	}
}