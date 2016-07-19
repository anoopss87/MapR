package yelp.yelp;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopNHashTags
{
	/* Mapper */
	public static class HTagsMap extends Mapper<LongWritable, Text, Text, IntWritable>
	{		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			//from Twitter			
			String[] TwittData = value.toString().split("\\s");
			
			//Identity Mapper
			if (TwittData.length == 2)
			{				
				context.write(new Text(TwittData[0]), new IntWritable(Integer.parseInt(TwittData[1])));
			}		
		}		
	}

	/* Reducer */
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>
	{		
		HashMap<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();
		public void reduce(Text key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException
		{		
			int count=0;
			
			for(IntWritable t : values)
			{
				count += t.get();
			}
			countMap.put(new Text(key),new IntWritable(count));						
		}
		
		@Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {			
			Map<Text, IntWritable> sortedMap = sortByValues(countMap);

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
	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            @SuppressWarnings("unchecked")
            
            //decreasing order
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
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
		job.setJarByClass(TopNHashTags.class);
	   
		job.setMapperClass(HTagsMap.class);
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
		
		// hadoop jar Yelp-0.0.1-SNAPSHOT.jar yelp.yelp.TopNHashTags /user/ass150430/Twitter_HashTag/ /user/ass150430/TopHTOutput
	}
}
