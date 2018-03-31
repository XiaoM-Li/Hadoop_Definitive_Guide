package hadoop.sort.first;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hadoop.tool.NcdcRecordParser;

public class SortDataPreprocessor extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SortDataPreprocessor(), args);

	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job=Job.getInstance(getConf());
		job.setJarByClass(SortDataPreprocessor.class);
		
		job.setMapperClass(CleanerMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		FileOutputFormat.setCompressOutput(job, true);
//		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)?0:1;
	}
	
	static class CleanerMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		private NcdcRecordParser parser=new NcdcRecordParser();
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			
			parser.parse(value);
			if(parser.isValidTemperature()){
				context.write(new IntWritable(parser.getAirTemperature()), value);
			}

		}
	}
}
