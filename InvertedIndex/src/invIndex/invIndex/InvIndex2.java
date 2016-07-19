package invIndex.invIndex;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvIndex2
{
	final static String STOP_WORDS_FILE = "stop-word-list.txt";
	/* Mapper */
	public static class InvIndexMap extends Mapper<LongWritable, Text, TupleS, IntWritable>
	{
		private Map<String, Map<String, Integer>> cMap = new HashMap<String, Map<String, Integer>>();
		Set<String> stopWords = new HashSet<String>();
		
		@Override
        protected void setup(Context context) throws IOException
		{
			Configuration conf = context.getConfiguration();
            String stp_file_name = conf.get(STOP_WORDS_FILE);
			Path pt=new Path("hdfs:" + stp_file_name);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br;
			try
            {
				br=new BufferedReader(new InputStreamReader(fs.open(pt)));                
            } 
            catch (FileNotFoundException e)
            {
                e.printStackTrace();
                throw new RuntimeException("Could not open stopwords file ",e);
            }
            String word;
            try 
            {
                while((word =br.readLine()) != null)
                {
                    stopWords.add(word);
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();                
                throw new RuntimeException("error while reading stopwords",e);
            }
            br.close();
        }
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{			
			//from business
			if(!value.toString().isEmpty())
			{
				String tokens = "[_|%$#+<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
				String tok = "\\s+|\\d+";
				String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
				cleanLine = cleanLine.replaceAll(tok, " ");
				String[] words = StringUtils.split(cleanLine.toString()," ");	
				
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				String fileName = fileSplit.getPath().getName();
				
				for(int i=0;i<words.length;++i)
				{
					if(stopWords.contains(words[i].trim()))
						continue;
					
					Map<String, Integer> temp = new HashMap<String, Integer>();					
					if(cMap.containsKey(words[i]))
					{						
						temp = cMap.get(words[i]);
						if(temp.containsKey(fileName))
						{
							temp.put(fileName, temp.get(fileName)+1);
							cMap.put(words[i], temp);
						}
						else
						{
							temp.put(fileName, 1);
							cMap.put(words[i], temp);
						}
					}
					else
					{
						temp.put(fileName, 1);
						cMap.put(words[i], temp);
					}					
				}				
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException
        {
			for (String k : cMap.keySet())
            {
            	Map<String, Integer> temp = new HashMap<String, Integer>();
            	temp = cMap.get(k);
            	for(String f : temp.keySet())
            	{
            		context.write(new TupleS(k, f), new IntWritable(temp.get(f)));
            	}
            }
        }
	}
	
	public class InvIndexPartitioner extends Partitioner<TupleS, IntWritable> 
	{
		@Override
		public int getPartition(TupleS key, IntWritable value, int numPartitions)
		{
			String word = key.getWord();			
			return (word.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	/* Reducer */
	public static class Reduce extends Reducer<TupleS,IntWritable,Text,Text>
	{
		private String prev = "";
		private ArrayList<TupleI> aList = new ArrayList<TupleI>();
		public void reduce(TupleS key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException
		{
			int count = 0;
			if(!(key.getWord().equals(prev)) && !prev.isEmpty())
			{
				StringBuilder str = new StringBuilder();
	            for (TupleI k : aList)
	            {
	            	str.append(k.getFileID() + ":" + k.getCount() + "`");                               
	            }
	            String val = str.substring(0, str.length()-1);
				context.write(new Text(prev), new Text(val));
				aList.clear();
			}
			for(IntWritable c : values)
			{
				count += c.get();
			}				
            aList.add(new TupleI(key.getDoc(), count));
            prev = key.getWord();
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException
        {
			StringBuilder str = new StringBuilder();
            for (TupleI k : aList)
            {
            	str.append(k.getFileID() + ":" + k.getCount() + "`");                               
            }
            String val = str.substring(0, str.length()-1);
			context.write(new Text(prev), new Text(val));
        }
	}
	
	// Driver program
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();// get all args
		if (otherArgs.length < 2)
		{
			System.err.println("Usage: jar <in> <out>");
			System.exit(2);
		}
			  
		Job job = Job.getInstance(conf, "CountYelp");
		
		if(otherArgs.length > 2)
            job.getConfiguration().set(STOP_WORDS_FILE, otherArgs[2]);
		
		job.setJarByClass(InvIndex2.class);
	   
		job.setMapperClass(InvIndexMap.class);
		job.setReducerClass(Reduce.class);
		
		// set output key type
		job.setMapOutputKeyClass(TupleS.class);
		job.setOutputKeyClass(Text.class);		
		
		// set output value type
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputValueClass(Text.class);		
				
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));		
		
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
		//hadoop jar InvertedIndex-0.0.1-SNAPSHOT.jar invIndex.invIndex.InvIndex2 /user/ass150430/Books /user/ass150430/InvIndex2 /user/ass150430/stop-word-list.txt
	}
}