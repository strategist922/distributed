package distributed.training;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class MyReducer extends MapReduceBase implements Reducer<IntWritable, TupleWritable, Text, IntWritable>{

	@Override
	public void reduce(IntWritable key, Iterator<TupleWritable> value,
			OutputCollector<Text, IntWritable> out, Reporter reporter)
			throws IOException {
		out.collect(new Text(value.next().toString()), new IntWritable(0));
		TupleWritable max=value.next();
		while (value.hasNext()) {
			max=value.next();
		}
		out.collect(new Text(max.toString()), new IntWritable(1));
	}

}
