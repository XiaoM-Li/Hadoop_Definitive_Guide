package hadoop.withcounters.tem;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hadoop.tool.NcdcRecordParser;

public class MaxTemperatureWithCounters extends Configured implements Tool {

	enum Temperature {
		MISSIING
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MaxTemperatureWithCounters(), args);

	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf());
		job.setJarByClass(MaxTemperatureWithCounters.class);

		job.setMapperClass(MaxTemperatureMapperWithCounters.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setCombinerClass(MaxTemperatureReducer.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	static class MaxTemperatureMapperWithCounters extends Mapper<LongWritable, Text, Text, IntWritable> {
		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			parser.parse(value);
			if (parser.isValidTemperature()) {
				context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
			} else if (parser.isMissingTemperature()) {
				context.getCounter("Air Temperature Records",Temperature.MISSIING.name()).increment(1);
			}
			context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
		}
	}

	static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(key, new IntWritable(maxValue));
		}

	}
}
