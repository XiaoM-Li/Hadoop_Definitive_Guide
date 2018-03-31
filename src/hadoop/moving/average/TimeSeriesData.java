package hadoop.moving.average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TimeSeriesData implements Writable{

	private long timeStamp;
	private double value;
	
	public TimeSeriesData() {
		
	}
	public TimeSeriesData(long timeStamp, double value) {
		this.timeStamp = timeStamp;
		this.value = value;
	}
	
	
	public long getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.timeStamp=in.readLong();
		this.value=in.readDouble();
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(timeStamp);
		out.writeDouble(value);
		
	}
	
	
}
