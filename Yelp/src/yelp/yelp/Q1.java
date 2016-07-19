package yelp.yelp;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

public class Q1
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
				StringBuilder val = new StringBuilder();				
				val.append(businessData[1]).append("\t").append(businessData[2]);
				//businessId -> address and category
				context.write(new TupleI(businessData[0], 0), new Text(val.toString()));
			}
			else if(fileName.startsWith("review"))
			{
				String delims = "^";
				String[] reviewData = StringUtils.split(value.toString(),delims);				
				//businessId -> rating
				context.write(new TupleI(reviewData[2], 1), new Text(reviewData[3]));
			}				
		}		
	}
	
	public class Q1Partitioner extends Partitioner<TupleI, Text> 
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
		HashMap<Text, DoubleWritable> resMap = new HashMap<Text, DoubleWritable>();
		
		double sum = 0;
		int count = 0;
		int callCount = 0;
		int iterCount = 0;
		
		public void reduce(TupleI key, Iterable<Text> values,Context context ) throws IOException, InterruptedException
		{
			//review table
			if(key.getId() == 1)
			{				
				if(rMap.containsKey(key.getStr()))
				{
					callCount++;
					for(Text t : values)
					{					
						sum += Double.parseDouble(t.toString());
						count++;
						iterCount++;
					}
					
					for(String s : rMap.get(key.getStr()))
					{						
						resMap.put(new Text(key.getStr() + "\t" + s), new DoubleWritable(sum / count));
					}
					rMap.clear();					
					count = 0;
					sum = 0;
				}
			}			
			else //business table
			{
				HashSet<String> temp = new HashSet<String>();
				for(Text t : values)
				{
					temp.add(t.toString());				
				}
				rMap.put(key.getStr(), temp);
			}					
		}
		
		@Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
			context.write(new Text(String.valueOf(callCount)), new DoubleWritable((double)iterCount));
			Map<Text, DoubleWritable> sortedMap = sortByValues(resMap);

            int counter = 0;
            for (Text key : sortedMap.keySet())
            {
                if (counter == 10)
                {
                    break;
                }
                context.write(key, sortedMap.get(key));
                counter++;
            }
        }
	}
	
	@SuppressWarnings("rawtypes")
	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map)
	{
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>()
        {
            @SuppressWarnings("unchecked")            
            
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2)
            {
            	//decreasing order
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : entries)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
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
		job.setJarByClass(Q1.class);
	   
		job.setMapperClass(GenMap.class);
		job.setReducerClass(Reduce.class);
		
		// set reduce key type		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		// set map value type
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(TupleI.class);	
		
		job.setPartitionerClass(Q1Partitioner.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		//hadoop jar YelpJoin-0.0.1-SNAPSHOT.jar yelp.yelp.Q1 /user/ass150430/Yelp_Q1 /user/ass150430/Q1_OP
	}
}
