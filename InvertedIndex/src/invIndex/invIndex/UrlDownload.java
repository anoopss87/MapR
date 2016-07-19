package invIndex.invIndex;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IOUtils;

public class UrlDownload
{
	public static void parse(Configuration conf, String dest) throws IOException
	{
		ArrayList<String> al = new ArrayList<String>();
		
		al.add("http://www.gutenberg.org/cache/epub/345/pg345.txt");
		al.add("http://www.gutenberg.org/files/4300/4300-0.txt");
		al.add("http://www.gutenberg.org/cache/epub/1400/pg1400.txt");
		al.add("http://www.gutenberg.org/files/5000/5000-8.txt");
		al.add("http://www.gutenberg.org/cache/epub/6130/pg6130.txt");
		al.add("http://www.gutenberg.org/cache/epub/730/pg730.txt");
		al.add("http://www.gutenberg.org/files/2600/2600-0.txt");
		al.add("http://www.gutenberg.org/cache/epub/1260/pg1260.txt");
		al.add("http://www.gutenberg.org/cache/epub/98/pg98.txt");
		al.add("http://www.gutenberg.org/cache/epub/236/pg236.txt");
		
		InputStream in = null;
		OutputStream out = null;	
		
		try
		{
			for(int i=0;i<al.size();++i)
			{
				String src = al.get(i);			
	    
				String[] temp = src.split("/");
				String dst = dest;//hdfs://cshadoop1/user/ass150430
				dst += temp[temp.length-1];
			
				FileUtils.copyURLToFile(new URL(src), new File(dst));				
			}
		}
		finally
		{					
			IOUtils.closeStream(out);
			IOUtils.closeStream(in);
		}	
	}
	public static void main(String[] args) throws IOException
	{		
		Configuration conf = new Configuration(); 
		parse(conf, args[0]);
		//hadoop jar InvertedIndex-0.0.1-SNAPSHOT.jar invIndex.invIndex.UrlDownload /user/ass150430/gutenberg
	}
}
