package hadoop.avro.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Test;

public class GenericWriteAvroFile {

	@Test
	public void test() throws IOException {
		Schema.Parser parser = new Schema.Parser();
		InputStream in = new FileInputStream("E://软件//Avro相关Jar包//user.avsc");
		Schema schema = parser.parse(in);
		GenericRecord user = new GenericData.Record(schema);
		user.put("name", "张三");
		user.put("age", 30);
		user.put("email", "zhangsan@*.com");

		File diskFile = new File("d://users.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, diskFile);
		dataFileWriter.append(user);
		dataFileWriter.close();

		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();//可以不指定模式，因为Avro数据文件的头部中有数据模式
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(diskFile, datumReader);
		GenericRecord _current = null;
		while (dataFileReader.hasNext()) {
			_current = dataFileReader.next();
			System.out.println(user);
		}

		dataFileReader.close();

	}

}
