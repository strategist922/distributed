package distributed.training;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class Sort {

	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{

		@Override
		public void map(
				LongWritable key, 
				Text value, 
				OutputCollector<IntWritable, Text> out, 
				Reporter reporter)
		throws IOException {
			StringTokenizer token = new StringTokenizer(value.toString());
			String current;
			while(token.hasMoreTokens()){
				current=token.nextToken();
				System.out.println(current.length()+":"+current);
				out.collect(new IntWritable(current.length()), new Text(current));
			}
			
		}
		
	}
	
	public static class MyReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{

		@Override
		public void reduce(
				IntWritable key, 
				Iterator<Text> value, 
				OutputCollector<IntWritable, Text> out, 
				Reporter reporter)
		throws IOException {
			while(value.hasNext()){
				out.collect(key, value.next());
			}			
		}
		
	}
	public static void main(String[] args) throws IOException {
		if(args.length<2){
			System.err.println("I need 2 arguments to work!!!");
			return;
		}
		JobConf conf = new JobConf(Sort.class);
		conf.setJobName("sort words by size");
		
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		JobClient.runJob(conf);
	}

}
