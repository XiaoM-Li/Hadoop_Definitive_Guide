package hadoop.joinouterleft;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinOuterLeftStepTwo {

	public static void main(String[] args) throws Exception {
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(JoinOuterLeftStepTwo.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(StepTwoReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

	}

	static class StepTwoReducer extends Reducer<Text, Text, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			HashSet<Text> set=new HashSet<>();
			System.out.println(key);
			for(Text value:values){
				set.add(value);
				System.out.println("添加元素"+value+"\t"+"集合长度"+set.size());
			}
			int number=set.size();
			context.write(key, new IntWritable(number));
		}
	}
}
