package hadoop.avro.tem;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class ReadResult {

	public static void main(String[] args) throws IOException {
		

		
	DatumReader<GenericRecord> reader=new GenericDatumReader<>();//不需要模式，文件中已经存的有
	DataFileReader<GenericRecord> fileReader=new DataFileReader<>(new File("d://avrotem//part-r-00000.avro"), reader);
	
	while(fileReader.hasNext()){
		System.out.println(fileReader.next());
	}
	//也可以使用增强for循环，fileReader可以迭代
	
	System.out.println(fileReader.getSchema());
	}

}
