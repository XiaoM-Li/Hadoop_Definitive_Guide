package hadoop.moving.average;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovingAverageReducer extends Reducer<CompositeKey, TimeSeriesData, Text, Text>{

	private int windowSize=4;
	@Override
	protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values,Context context)throws IOException, InterruptedException {
		
		double sum=0.0;
		
		
	}
}
