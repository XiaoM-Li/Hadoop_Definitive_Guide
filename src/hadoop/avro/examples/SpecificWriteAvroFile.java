package hadoop.avro.examples;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class SpecificWriteAvroFile {

	public static void main(String[] args) throws IOException {
		// TODO 自动生成的方法存根
		User u1 = new User();
		u1.setName("Tom");
		u1.setAge(24);
		u1.setEmail("Tom@163.com");

		User u2 = new User("Jerry", 25, "Jerry@qq.com");

		User u3 = User.newBuilder().setAge(30).setName("Messi").setEmail("Messi@gmail.com").build();

		String path = "D://user.avro";
		DatumWriter<User> datumWriter = new SpecificDatumWriter<User>(User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<>(datumWriter);
		dataFileWriter.create(u1.getSchema(), new File(path));
		
		dataFileWriter.append(u1);
		dataFileWriter.append(u2);
		dataFileWriter.append(u3);
		
		dataFileWriter.close();
	}

}
