package hadoop.joinouterleft;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import hadoop.tool.TextPair;

public class UserMapper extends Mapper<LongWritable, Text, TextPair, TextPair>{
	
	private TextPair keyout;
	private TextPair valueout;
	@Override
	protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		
		String[] fileds=StringUtils.split(value.toString(), "\t");
		keyout=new TextPair(fileds[0],"1");
		valueout=new TextPair(fileds[1], "L");
		
		context.write(keyout, valueout);
	}
	
}
