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

public class MondrianMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

	private String message="\t\t";
	
	/**	This method is used for initializing mapper before first run.	*/
	public void configure(JobConf conf){
		System.out.println(this.message+"Initializing mapper");
	}
	
	/**	Main mapper method. Data is read from the stdin and mapper writes the results to the filesystem.	*/
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> out, Reporter reporter)
			throws IOException {
		
	}
	
	/**	This method is used for de-initializing mapper before the mapper object is destroyed.	*/
	public void close(){
		System.out.println(this.message+"Killing mapper");
	}
	
}
