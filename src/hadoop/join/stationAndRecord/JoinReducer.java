package hadoop.join.stationAndRecord;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import hadoop.tool.TextPair;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text>{
	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
		
		Iterator<Text> iterator = values.iterator();
		Text stationName=new Text(iterator.next());
		while(iterator.hasNext()){
			Text record=iterator.next();
			Text outValue=new Text(stationName.toString()+"\t"+record.toString());
			context.write(key.getFirst(), outValue);
		}
		
//		StringBuffer stringBuffer=new StringBuffer();
//		Iterator<Text> iterator = values.iterator();
//		while(iterator.hasNext()){
//			stringBuffer.append(iterator.next().toString()+"\t");
//		}
//		context.write(new Text(key.toString()), new Text(stringBuffer.toString()));
	}

}
