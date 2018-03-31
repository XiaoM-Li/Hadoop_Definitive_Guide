package hadoop.avro.tem;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hadoop.tool.NcdcRecordParser;

public class AvroGenericMaxtemperature extends Configured implements Tool{
	
	//private static final File schemaFile=new File("weather.avsc");
	private static final Schema SCHEMA=new Schema.Parser().parse(
			"{"+
			"\"type\":\"record\","+
			"\"name\":\"WeatherRecord\","+
			"\"doc\":\"A weather reading.\","+
			"\"fields\":["+
			"{\"name\":\"year\",\"type\":\"string\"},"+
			"{\"name\":\"temperature\",\"type\":\"int\"},"+
			"{\"name\":\"stationId\",\"type\":\"string\"}"+
			"]"+
			"}"
			);
	
	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new AvroGenericMaxtemperature(), args);
		
	}

	@Override
	public int run(String[] args) throws Exception {
		
		//SCHEMA=new Schema.Parser().parse(schemaFile);
		//System.out.println(SCHEMA);
		
		Job job=Job.getInstance(getConf());
		job.setJarByClass(AvroGenericMaxtemperature.class);
		
		job.getConfiguration().setBoolean( Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, SCHEMA);
		AvroJob.setOutputKeySchema(job, SCHEMA);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		
		job.setMapperClass(MaxTemMapper.class);
		job.setReducerClass(MaxTemReducer.class);
		
		job.waitForCompletion(true);
		return 0;
	}
	
	public static class MaxTemMapper extends Mapper<LongWritable, Text, AvroKey<String>, AvroValue<GenericRecord>>{
		private NcdcRecordParser parser=new NcdcRecordParser();
		private GenericRecord record=new GenericData.Record(SCHEMA);
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			parser.parse(value.toString());
			if(parser.isValidTemperature()){
				record.put("year", parser.getYear());
				record.put("temperature", parser.getAirTemperature());
				record.put("stationId", parser.getStationId());
				context.write(new AvroKey<String>(parser.getYear()), new AvroValue<GenericRecord>(record));
			}
		}
	}
	
	public static class MaxTemReducer extends Reducer<AvroKey<String>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable>{
		@Override
		protected void reduce(AvroKey<String> key, Iterable<AvroValue<GenericRecord>> values,Context context)throws IOException, InterruptedException {
			GenericRecord max=null;
			for(AvroValue<GenericRecord> value:values){
				GenericRecord record=value.datum();
				if(max==null||(Integer)record.get("temperature")>(Integer)max.get("temperature")){
					max=newWeatherRecord(record);
				}
			}
			
		}
	}
	private static GenericRecord newWeatherRecord(GenericRecord value){
		GenericRecord record= new GenericData.Record(SCHEMA);
		record.put("year",value.get("year"));
		record.put("temperature",value.get("temperature"));
		record.put("stationId",value.get("stationId"));
		return record;
	}
	
}
