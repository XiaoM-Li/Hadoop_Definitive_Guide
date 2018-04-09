package hadoop.avro.examples;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

public class SpecificReadAvroFile {

	public static void main(String[] args) throws IOException {
		
		DatumReader<User> reader=new SpecificDatumReader<>(User.class);
		DataFileReader<User> fileReader=new DataFileReader<>(new File("D://user.avro"), reader);
		User user=null;
		while(fileReader.hasNext()){
			user=fileReader.next();
			System.out.println(user);
		}
		fileReader.close();

	}

}
