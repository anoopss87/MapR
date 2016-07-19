package yelp.yelp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class WordPair implements WritableComparable<WordPair>
{
	private String w1;
	private String w2;
	
	WordPair()
	{
		w1 = "";
		w2 = "";
	}
	WordPair(String a, String b)
	{
		w1 = a;
		w2 = b;
	}
	public int compareTo(WordPair other)
	{
		int retVal = w1.compareTo(other.w1);
		if(retVal != 0)
		{
			return retVal;
		}
		if(w2.equals("*"))
			return -1;
		else if(other.w2.equals("*"))
			return 1;
		return w2.compareTo(other.w2);
	}
	public void readFields(DataInput arg0) throws IOException
	{
		this.w1=arg0.readUTF();
	    this.w2=arg0.readUTF();		
	}
	public void write(DataOutput arg0) throws IOException
	{
		arg0.writeUTF(w1);
		arg0.writeUTF(w2);		
	}
	
	public String getW1()
	{
		return w1;
	}
	public String getW2()
	{
		return w2;
	}
	
}
