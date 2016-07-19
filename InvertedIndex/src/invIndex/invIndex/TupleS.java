package invIndex.invIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TupleS implements WritableComparable<TupleS>
{
	private String word;
	private String doc;
	
	TupleS()
	{
		word = "";
		doc = "";
	}
	TupleS(String a, String b)
	{
		word = a;
		doc = b;
	}
	public int compareTo(TupleS other)
	{
		if(word.compareTo(other.word) == 0)
		{
			return doc.compareTo(other.doc);
		}
		else
		{
			return word.compareTo(other.word);
		}
	}	
	
	public void readFields(DataInput arg0) throws IOException
	{
		this.word=arg0.readUTF();
	    this.doc=arg0.readUTF();
	}
	public void write(DataOutput arg0) throws IOException
	{
		arg0.writeUTF(word);
		arg0.writeUTF(doc);
	}
	
	public String getWord()
	{
		return word;
	}
	public String getDoc()
	{
		return doc;
	}	
}

