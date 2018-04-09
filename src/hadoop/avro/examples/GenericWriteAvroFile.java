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
		InputStream in = new FileInputStream("E://���//Avro���Jar��//user.avsc");
		Schema schema = parser.parse(in);
		GenericRecord user = new GenericData.Record(schema);
		user.put("name", "����");
		user.put("age", 30);
		user.put("email", "zhangsan@*.com");

		File diskFile = new File("d://users.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, diskFile);
		dataFileWriter.append(user);
		dataFileWriter.close();

		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();//���Բ�ָ��ģʽ����ΪAvro�����ļ���ͷ����������ģʽ
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(diskFile, datumReader);
		GenericRecord _current = null;
		while (dataFileReader.hasNext()) {
			_current = dataFileReader.next();
			System.out.println(user);
		}

		dataFileReader.close();

	}

}
