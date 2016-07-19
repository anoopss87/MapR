package yelp.yelp;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CoOccurencePair
{
	/* Mapper */
	public static class CoOccurMap extends Mapper<LongWritable, Text, WordPair, IntWritable>
	{		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			//from business
			if(!value.toString().isEmpty())
			{
				String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
				String tok = "[\\s+]";
				String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
				cleanLine = cleanLine.replaceAll(tok, " ");
				String[] words = StringUtils.split(cleanLine.toString()," ");			
				for(int i=0;i<words.length;++i)
				{
					for(int j=i+1;j<words.length;++j)
					{
						context.write(new WordPair(words[i], words[j]), new IntWritable(1));
					}
					context.write(new WordPair(words[i], "*"), new IntWritable(words.length - (i+1)));
				}
			}
		}		
	}
	
	public class WordPairPartitioner extends Partitioner<WordPair,IntWritable> {

	    public int getPartition(WordPair wordPair, IntWritable intWritable, int numPartitions) {
	        return wordPair.getW1().hashCode() % numPartitions;
	    }
	}

	/* Reducer */
	public static class Reduce extends Reducer<WordPair,IntWritable,Text,DoubleWritable>
	{		
		private DoubleWritable totalCount = new DoubleWritable();
	    private DoubleWritable relativeCount = new DoubleWritable();
	    private Text currentWord = new Text("NOT_SET");
	    
		public void reduce(WordPair key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException
		{		
			if(key.getW2().equals("*"))
			{
				if(key.getW1().equals(currentWord))
					totalCount.set(totalCount.get() + getTotalCount(values));
				else
				{
					currentWord.set(key.getW1());
					totalCount.set(0);
					totalCount.set(getTotalCount(values));
				}
			}
			else
			{
				int count = getTotalCount(values);
				relativeCount.set((double) count / totalCount.get());
	            context.write(new Text("(" + key.getW1() + "," + key.getW2() + ")"), relativeCount);
			}
		}
		
		private int getTotalCount(Iterable<IntWritable> values) {
	        int count = 0;
	        for (IntWritable value : values) {
	            count += value.get();
	        }
	        return count;
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
		job.setJarByClass(CoOccurencePair.class);
	   
		job.setMapperClass(CoOccurMap.class);
		job.setReducerClass(Reduce.class);
		
		// set output key type
		job.setMapOutputKeyClass(WordPair.class);
		job.setOutputKeyClass(Text.class);		
		
		// set output value type
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setPartitionerClass(WordPairPartitioner.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		// hadoop jar Yelp-0.0.1-SNAPSHOT.jar yelp.yelp.CoOccurencePair /user/ass150430/WordCoOccurInput /user/ass150430/WordCoOccurOutput
	}
}

