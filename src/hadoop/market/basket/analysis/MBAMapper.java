package hadoop.market.basket.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MBAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public static final int DEFAULT_NUMBER_OF_PAIRS=2;
	private IntWritable ONE = new IntWritable(1);
	private Text outKey = new Text();
	private List<String> list = new ArrayList<>();
	private List<List<String>> combinations = new ArrayList<>();
	private Logger log=Logger.getLogger(MBAMapper.class);
	private int numberofPairs;
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {

		this.numberofPairs=context.getConfiguration().getInt("number.of.pairs", DEFAULT_NUMBER_OF_PAIRS);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		list = MBATool.StringToSortedList(line);
		combinations = MBATool.findSortedCombinations(list, numberofPairs);
		for(List<String> combination:combinations){
			outKey.set(combination.toString());
			log.info("Mapº¯ÊýÐ´³ö"+outKey+":"+ONE);
			context.write(outKey, ONE);
		}
	}

}
