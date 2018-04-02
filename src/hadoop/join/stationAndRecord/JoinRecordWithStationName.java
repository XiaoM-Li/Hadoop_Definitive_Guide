package hadoop.join.stationAndRecord;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hadoop.tool.TextPair;

public class JoinRecordWithStationName extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new JoinRecordWithStationName(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf());
		job.setJarByClass(getClass());
		

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinRecordMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinStationMapper.class);
		job.setReducerClass(JoinReducer.class);
		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true)?0:1;
	}

	public static class KeyPartitioner extends Partitioner<TextPair, Text> {

		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

	}

}
