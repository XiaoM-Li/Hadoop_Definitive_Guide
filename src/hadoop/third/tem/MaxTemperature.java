package hadoop.third.tem;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperature extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new MaxTemperature(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO 自动生成的方法存根
		Job job=Job.getInstance(getConf());
		
		job.setJarByClass(MaxTemperature.class);
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		job.setCombinerClass(MaxTemperatureReducer.class);//设置Combiner
		
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(CombineTextInputFormat.class);//输入使用CombineFileInputFormat,更好处理小文件
		
		FileOutputFormat.setCompressOutput(job, true);//输出设置压缩
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		return job.waitForCompletion(true)?0:1;
	}

}
