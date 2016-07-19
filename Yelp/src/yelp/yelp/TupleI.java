package yelpjoin.yelpjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TupleI implements WritableComparable<TupleI>
{
	private String str;
	private Integer id;
	
	TupleI()
	{
		str = "";
		id = 0;
	}
	TupleI(String a, Integer b)
	{
		str = a;
		id = b;
	}
	public int compareTo(TupleI other)
	{
		if(str.compareTo(other.str) == 0)
		{
			return id.compareTo(other.id);
		}
		else
		{
			return str.compareTo(other.str);
		}
	}	
	
	public void readFields(DataInput arg0) throws IOException
	{
		this.str=arg0.readUTF();
	    this.id=arg0.readInt();
	}
	public void write(DataOutput arg0) throws IOException
	{
		arg0.writeUTF(str);
		arg0.writeInt(id);
	}
	
	public String getStr()
	{
		return str;
	}
	public Integer getId()
	{
		return id;
	}	
}
