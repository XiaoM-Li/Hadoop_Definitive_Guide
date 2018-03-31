package hadoop.joinouterleft;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import hadoop.tool.TextPair;

public class ProductionMapper extends Mapper<LongWritable, Text, TextPair, TextPair>{
	
	private TextPair keyout;
	private TextPair valueout;
	@Override
	protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		
		String[] fileds=StringUtils.split(value.toString(), "\t");
		keyout=new TextPair(fileds[2],"2");
		valueout=new TextPair(fileds[1], "P");
		
		context.write(keyout, valueout);
	}
}
