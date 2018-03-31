package hadoop.market.basket.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MBARunner {

	public static void main(String[] args) throws Exception {
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(MBARunner.class);
		
		job.setMapperClass(MBAMapper.class);
		
		job.setCombinerClass(MBAReducer.class);
		
		job.setReducerClass(MBAReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int numberOfPairs=Integer.parseInt(args[2]);
		job.getConfiguration().setInt("number.of.pairs", numberOfPairs);
		
		FileSystem.get(job.getConfiguration()).delete(new Path("D://MBA"), true);
		
		job.waitForCompletion(true);

	}

}
