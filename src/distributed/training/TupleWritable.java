package distributed.training;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public class TupleWritable implements WritableComparable<TupleWritable>{

	private String[] data;
	private int[] value;
	private int[] qid;
	
	public static int UNINITIALIZED=-9999;
	public static String SEPARATOR=",";
	private static int SIZE_OF_BUFFER=32;
	private static char END_OF_WORD='\0';
	
	public TupleWritable() {
		//System.out.println("\t\t\tDefault Contructor");
		
	}
	
	public TupleWritable(String[] data) {
		this.data=data;
		this.value = new int[this.data.length];
		for(int i=0;i<this.value.length;i++){
			this.value[i]=UNINITIALIZED;
		}
	}
	
	public void setQid(int qid[]){
		this.qid=qid;
	}
	
	public IntWritable getDistanceFromZero(){
		int sum=0;
		for(int dim:this.qid){
			sum+=getValue(dim);
		}
		return new IntWritable(sum);
	}
	
	public int getValue(int dim){
		if(this.value[dim-1]==UNINITIALIZED)
			this.value[dim-1]=new Integer(this.data[dim-1].trim());
		return this.value[dim-1];
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.data = new String[in.readInt()];
		char[] word = new char[SIZE_OF_BUFFER];
		for(int j=0;j<this.data.length;j++)
			for(int i=0;;i++){
				word[i]=in.readChar();
				if(word[i]==END_OF_WORD)
					break;
				
				this.data[j]=new String(word,0,i+1);
			}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//System.out.println("\t\t\t(TupleWritable) Write:"+this.toString());
		out.writeInt(this.data.length);
		for(String s:this.data){
			for(int i=0;i<s.length();i++)
				out.writeChar(s.charAt(i));
			out.writeChar(END_OF_WORD);
		}
	}

	@Override
	public int compareTo(TupleWritable o) {
		if(this.getDistanceFromZero().get()>o.getDistanceFromZero().get())
			return 1;
		else if(this.getDistanceFromZero().get()<o.getDistanceFromZero().get())
			return -1;
		else
			return 0;
	}
	
	@Override
	public String toString(){
		if(this.data==null)
			return "()";
		
		String buffer="(";
		for(int i=0;i<this.data.length;i++)
			if(i==this.data.length-1)
				buffer+=this.data[i]+")";
			else
				buffer+=this.data[i]+",";
		return buffer;
	}
	

}
