package hadoop.avro.examples;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

public class TestAvroSpecificMapping {

	/*
	 * 这个例子中使用Avro的命令行工具来为一个模式生成代码
	 * 在序列化和反序列化的代码中，通过构建一个StringPair的实例来替代GenericRecord对象。
	 */
	@Test
	public void test() throws IOException{
		
		StringPair datum=new StringPair();
		datum.setLeft("L");
		datum.setRight("R");
		
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		SpecificDatumWriter<StringPair> writer=new SpecificDatumWriter<>(StringPair.class);
		Encoder encoder=EncoderFactory.get().binaryEncoder(out, null);
		writer.write(datum, encoder);//这里是具体的StringPair的实例
		encoder.flush();
		out.close();
		
		DatumReader<StringPair> reader=new SpecificDatumReader<StringPair>(StringPair.class);
		Decoder decoder=DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		StringPair result = reader.read(null, decoder);
		
		System.out.println(result.getLeft());
		System.out.println(result.getRight());
	}
	
}
