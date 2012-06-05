package method.mondrian;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import database.Tuple;

public class MondrianMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Tuple>{

	
	private String message="\t\t";
	private int[] qid;
	/**	This method is used for initializing mapper before first run.	*/
	public void configure(JobConf conf){
		String [] qid=conf.get("qid").split(" ");
		this.qid=new int[qid.length];
		for(int i=0;i<this.qid.length;i++)
			this.qid[i]=new Integer(qid[i]);
		System.out.println(this.message+"Initializing mapper");
	}
	
	/**	Main mapper method. Data is read from the stdin and mapper writes the results to the filesystem.	*/
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Tuple> out, Reporter reporter)
			throws IOException {
		Tuple temp = new Tuple(value.toString().split(","),this.qid);
		out.collect(new IntWritable(temp.getDistance()),temp);
	}
	
	/**	This method is used for de-initializing mapper before the mapper object is destroyed.	*/
	public void close(){
		System.out.println(this.message+"Killing mapper");
	}
	
}
