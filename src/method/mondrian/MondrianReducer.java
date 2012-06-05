package method.mondrian;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import database.Tuple;

public class MondrianReducer extends MapReduceBase implements Reducer<IntWritable, Tuple, Text, IntWritable>{

	private String message="\t\t";
	private int[] qid;
	/**	This method is used for initializing mapper before first run.	*/
	public void configure(JobConf conf){
		String [] qid=conf.get("qid").split(" ");
		this.qid=new int[qid.length];
		for(int i=0;i<this.qid.length;i++)
			this.qid[i]=new Integer(qid[i]);
		System.out.println(this.message+"Initializing reducer");
	}
	
	/**	Main reducer method. Exit data from mapper is read and final data is written to the filesystem.	*/
	@Override
	public void reduce(IntWritable key, Iterator<Tuple> values,
			OutputCollector<Text, IntWritable> out, Reporter reporter)
			throws IOException {
		while(values.hasNext()){
			Tuple current=values.next();
			out.collect(new Text(current.toString()), new IntWritable(current.getDistance()));
		}
	}
	
	/**	This method is used for de-initializing mapper before the mapper object is destroyed.	*/
	public void close(){
		System.out.println(message+"Killing reducer!");
	}
	

}
