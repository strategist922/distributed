package distributed.training;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, TupleWritable>{
	
	private int[] qid={1,2,3,4,5,6,7,8};
	
	public void configure(JobConf conf){					//Mapper initializer
		System.out.println("HelloWorld"+conf.get("idiotita"));
	}
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, TupleWritable> out, Reporter reporter)
			throws IOException {
		TupleWritable tup = new TupleWritable(value.toString().split(TupleWritable.SEPARATOR));
		tup.setQid(qid);
		System.out.println("\t\tMapper Writes:"+tup.toString());
		out.collect(new IntWritable(1),tup);
	}
	
	public void close(){									//Mapper de-initializer!
		System.out.println("I am dying re...");
	}
}
