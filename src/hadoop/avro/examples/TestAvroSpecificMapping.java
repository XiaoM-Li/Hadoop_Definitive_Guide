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
	 * ���������ʹ��Avro�������й�����Ϊһ��ģʽ���ɴ���
	 * �����л��ͷ����л��Ĵ����У�ͨ������һ��StringPair��ʵ�������GenericRecord����
	 */
	@Test
	public void test() throws IOException{
		
		StringPair datum=new StringPair();
		datum.setLeft("L");
		datum.setRight("R");
		
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		SpecificDatumWriter<StringPair> writer=new SpecificDatumWriter<>(StringPair.class);
		Encoder encoder=EncoderFactory.get().binaryEncoder(out, null);
		writer.write(datum, encoder);//�����Ǿ����StringPair��ʵ��
		encoder.flush();
		out.close();
		
		DatumReader<StringPair> reader=new SpecificDatumReader<StringPair>(StringPair.class);
		Decoder decoder=DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
		StringPair result = reader.read(null, decoder);
		
		System.out.println(result.getLeft());
		System.out.println(result.getRight());
	}
	
}
