package hadoop.joinouterleft;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.org.apache.bcel.internal.generic.NEW;

import hadoop.tool.TextPair;

public class JoinOuterLeft {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(JoinOuterLeft.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ProductionMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UserMapper.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(TextPair.class);

		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);

		job.setReducerClass(StepOneReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);

	}

	static class StepOneReducer extends Reducer<TextPair, TextPair, Text, Text> {
		
		Text location = new Text();
		Text production = new Text();
		@Override
		protected void reduce(TextPair key, Iterable<TextPair> values, Context context)
				throws IOException, InterruptedException {

			for (TextPair value : values) {
				if (value.getSecond().toString().equals("L")) {
					location.set(value.getFirst());
					continue;
				}
				if(value.getSecond().toString().equals("P")){
					production.set(value.getFirst());
					
				}
				context.write(production, location);
				
			}

		}

	}

	public static class KeyPartitioner extends Partitioner<TextPair, TextPair> {

		@Override
		public int getPartition(TextPair key, TextPair value, int numPartitions) {
			// TODO Auto-generated method stub
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

	}
}
