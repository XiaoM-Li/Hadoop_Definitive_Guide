package hadoop.tool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextPair implements WritableComparable<TextPair>{

	private Text first=new Text();
	private Text second=new Text();
	
	public TextPair() {
		// TODO Auto-generated constructor stub
	}
	
	public TextPair(Text first,Text second){
		this.first=first;
		this.second=second;
	}
	
	public TextPair(String first,String second){
		this.first=new Text(first);
		this.second=new Text(second);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(in);
		second.readFields(in);
	}

	public Text getFirst() {
		return first;
	}

	public void setFirst(Text first) {
		this.first = first;
	}

	public Text getSecond() {
		return second;
	}

	public void setSecond(Text second) {
		this.second = second;
	}
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if(obj instanceof TextPair){
			TextPair tp=(TextPair) obj;
			return first.equals(tp.first)&&second.equals(tp.second);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return first.hashCode()*163+second.hashCode();
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return first.toString()+"\t"+second.toString();
	}

	@Override
	public int compareTo(TextPair o) {
		// TODO Auto-generated method stub
		int cmp=first.compareTo(o.first);
		if(cmp!=0){
			return cmp;
		}
		return second.compareTo(o.second);
	}
	
	public static class FirstComparator extends WritableComparator{

		protected  FirstComparator() {
			super(TextPair.class,true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			
			TextPair tp1=(TextPair) a;
			TextPair tp2=(TextPair) b;
			//System.out.println("分组比较"+tp1.getFirst()+" "+tp2.getFirst()+" "+tp1.getFirst().compareTo(tp2.getFirst()));
			return tp1.getFirst().compareTo(tp2.getFirst());
		}
	}

}
