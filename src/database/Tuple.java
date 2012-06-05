package database;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Tuple implements WritableComparable<Tuple> {

	private String[] data;
	private int[] value;
	private int[] qid;
	
	private static int UNDEFINED=-999999;
	private char endOfWord='\n';
	
	public Tuple(){
		
	}
	
	public Tuple(String[] data, int[] qid){
		setData(data);
		setQid(qid);
	}
	
	public void setData(String[] data){
		this.data=data;
		this.value = new int[this.data.length];
		for(int i=0;i<this.data.length;i++)
			this.value[i]=UNDEFINED;
	}
	
	public int getValue(int dim){
		if(this.value[dim-1]==UNDEFINED)
			this.value[dim-1] = new Integer(this.data[dim-1].trim());
		return this.value[dim-1];
	}
	
	public void setQid(int[] qid){
		this.qid=qid;
	}
	
	public int getDistance(){
		int sum=0;
		for(int dim:this.qid)
			sum+=this.getValue(dim);
		return sum;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int dataSize=in.readInt();
		String[] data = new String[dataSize];
		for(int i=0;i<dataSize;i++)
			data[i]=in.readLine().trim();
		
		setData(data);
		
		int qidSize=in.readInt();
		this.qid = new int[qidSize];
		for(int i=0;i<qidSize;i++)
			this.qid[i]=in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.data.length);
		for(String s:data){
			out.writeUTF(s);
			out.writeChar(endOfWord);
		}
		
		out.writeInt(this.qid.length);
		for(int i:this.qid)
			out.writeInt(i);
	}
	
	@Override
	public int compareTo(Tuple otherTuple) {
		if(this.getDistance()<otherTuple.getDistance())
			return 1;
		else if(this.getDistance()>otherTuple.getDistance())
			return -1;
		else
			return 0;
	}
	
	@Override
	public String toString(){
		String buffer = "(";
		for(int i=0;i<this.data.length-1;i++)
			buffer+=this.data[i]+",";
		buffer+=this.data[this.data.length-1]+")";		
		/*
		 	buffer+= "\t(qid)[";
			for(int i=0;i<this.qid.length-1;i++)
			buffer+=this.qid[i]+",";
			buffer+=this.qid[this.qid.length-1]+"]";
		*/
		return buffer;
	}

}
