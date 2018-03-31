package hadoop.moving.average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey>{

	private String name;
	private long timeStamp;
	
	public CompositeKey() {
		
	}
	
	public CompositeKey(String name,long timeStamp){
		
		this.name=name;
		this.timeStamp=timeStamp;
	}
	
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	
	@Override
	public String toString() {
		return  name + "," + timeStamp ;
	}
	
	@Override
	public boolean equals(Object obj) {
		CompositeKey key=(CompositeKey) obj;
		if(this.name.equals(key.name)){
			return this.timeStamp==key.timeStamp;
		}
		return this.name.equals(key.name);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.name=in.readUTF();
		this.timeStamp=in.readLong();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(name);
		out.writeLong(timeStamp);
	}

	@Override
	public int compareTo(CompositeKey o) {
		// TODO Auto-generated method stub
		if(this.name.equals(o.name)){
			if(this.timeStamp<o.timeStamp){
				return -1;
			}
			if(this.timeStamp==o.timeStamp){
				return 0;
			}
			if(this.timeStamp>o.timeStamp){
				return 1;
			}
		}
		return this.name.compareTo(o.name);
	}

}
