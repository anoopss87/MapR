package yelp.yelp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Tuple implements WritableComparable<Tuple>
{
	private String zc;
	private Integer count;
	
	Tuple()
	{
		zc = "";
		count = 0;
	}
	Tuple(String a, Integer b)
	{
		zc = a;
		count = b;
	}
	public int compareTo(Tuple other)
	{
		if(zc.compareTo(other.zc) == 0)
		{
			return count.compareTo(other.count);
		}
		else
		{
			return zc.compareTo(other.zc);
		}
	}	
	
	public void readFields(DataInput arg0) throws IOException
	{
		this.zc=arg0.readUTF();
	    this.count=arg0.readInt();
	}
	public void write(DataOutput arg0) throws IOException
	{
		arg0.writeUTF(zc);
		arg0.writeInt(count);
	}
	
	public String getZC()
	{
		return zc;
	}
	public Integer getCount()
	{
		return count;
	}	
}
