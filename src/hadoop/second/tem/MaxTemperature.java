package hadoop.second.tem;
/*
 * ʹ�ý���������ĵڶ������
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

	public static void main(String[] args) throws Exception {
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(MaxTemperature.class);
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		job.setCombinerClass(MaxTemperatureReducer.class);//����Combiner
		
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(CombineTextInputFormat.class);//����ʹ��CombineFileInputFormat,���ô���С�ļ�
		
		
		FileInputFormat.setInputPaths(job, new Path("D://01_09"));
		FileOutputFormat.setOutputPath(job, new Path("D://01_09maxtem"));

		job.waitForCompletion(true);
	}

}
