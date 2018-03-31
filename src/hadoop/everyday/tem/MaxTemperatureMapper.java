package hadoop.everyday.tem;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import hadoop.tool.NcdcRecordParser;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private NcdcRecordParser parser=new NcdcRecordParser();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		parser.parse(value);
		if(parser.isValidTemperature()){
			context.write(new Text(parser.getYearMonthDay()), new IntWritable(parser.getAirTemperature()));
		}
	}

}
