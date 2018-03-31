package hadoop.join.stationAndRecord;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import hadoop.tool.StationMetaDataParser;
import hadoop.tool.TextPair;

public class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text>{

	private StationMetaDataParser parser=new StationMetaDataParser();
	@Override
	protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		parser.parse(value);
		context.write(new TextPair(parser.getStationID(), "0"), new Text(parser.getStationName()));
		
		
	}
}
