package hadoop.moving.average;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovingAverageMapper extends Mapper<LongWritable, Text, CompositeKey, TimeSeriesData>{
	
	private CompositeKey outkey=new CompositeKey();
	private TimeSeriesData outvalue=new TimeSeriesData();
	SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd");
	private Date date=new Date();
	@Override
	protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
		String line=value.toString();
		String[] fields=StringUtils.split(line, ",");
		try {
			date=dateFormat.parse(fields[1]);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			System.err.println("日期解析出现错误");
		}
		outkey.setName(fields[0]);
		outkey.setTimeStamp(date.getTime());
		outvalue.setTimeStamp(date.getTime());
		outvalue.setValue(Double.parseDouble(fields[2]));
		context.write(outkey, outvalue);
		
	}
}
