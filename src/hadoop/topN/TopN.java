package hadoop.topN;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopN {

	public static void main(String[] args) throws Exception {
		
		Job job=Job.getInstance(new Configuration());
		job.setJarByClass(TopN.class);
		
		job.setMapperClass(TopNMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		

	}

	static class TopNMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		private TreeMap<Integer, Text> topNmap=new TreeMap<>();
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String line=value.toString();
			String[] strings=line.split("\t");
			int temperature=Integer.parseInt(strings[1]);
			topNmap.put(temperature, value);
			//System.out.println(temperature+"\t"+value.toString());
			
		}
		
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)throws IOException, InterruptedException {
			
			for(Text value:topNmap.values()){
				context.write(value, NullWritable.get());
			}
		}
	}
}
