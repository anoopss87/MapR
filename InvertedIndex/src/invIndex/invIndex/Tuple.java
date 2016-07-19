package invIndex.invIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Tuple implements WritableComparable<Tuple>
{
	private String fileId;
	private Integer count;
	
	Tuple()
	{
		fileId = "";
		count = 0;
	}
	Tuple(String a, Integer b)
	{
		fileId = a;
		count = b;
	}
	public int compareTo(Tuple other)
	{
		if(fileId.compareTo(other.fileId) == 0)
		{
			return count.compareTo(other.count);
		}
		else
		{
			return fileId.compareTo(other.fileId);
		}
	}	
	
	public void readFields(DataInput arg0) throws IOException
	{
		this.fileId=arg0.readUTF();
	    this.count=arg0.readInt();
	}
	public void write(DataOutput arg0) throws IOException
	{
		arg0.writeUTF(fileId);
		arg0.writeInt(count);
	}
	
	public String getFileID()
	{
		return fileId;
	}
	public Integer getCount()
	{
		return count;
	}	
}

