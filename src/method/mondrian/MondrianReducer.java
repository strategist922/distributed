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

public class MondrianReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

	/**	This method is used for initializing mapper before first run.	*/
	public void configure(JobConf conf){
		
	}
	
	/**	Main reducer method. Exit data from mapper is read and final data is written to the filesystem.	*/
	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> out, Reporter reporter)
			throws IOException {
		
	}
	
	/**	This method is used for de-initializing mapper before the mapper object is destroyed.	*/
	public void close(){
		
	}
	

}
