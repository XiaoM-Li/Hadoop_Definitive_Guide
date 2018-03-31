package hadoop.sort.first;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortByTemperatureUsingTotalOrderPartitioner extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
			ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitioner(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job=Job.getInstance(getConf());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		InputSampler.Sampler<IntWritable, Text> sampler=new InputSampler.RandomSampler(0.1, 10000, 10);
		
		InputSampler.writePartitionFile(job, sampler);
		Configuration conf=job.getConfiguration();
		String partitionFile=TotalOrderPartitioner.getPartitionFile(conf);
		URI partitionUri=new URI(partitionFile);
		job.addCacheFile(partitionUri);
			
		return job.waitForCompletion(true)?0:1;
	}

}
