package hadoop.avro.examples;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;

/*
 * ʱ��2018.01.19
 * ��Ҫ���ܣ�Avro��ͨ��ӳ�䣨generic mapping��
 */

public class TestAvroGenericMapping {

	@Test
	public void test() throws IOException {  
        //��schema��StringPair.avsc�ļ��м���  
        Schema.Parser parser = new Schema.Parser();  
        Schema schema = parser.parse(getClass().getResourceAsStream("StringPair.avsc"));  
  
        //����schema����һ��recordʾ��  
        GenericRecord datum = new GenericData.Record(schema);  
        datum.put("left", "L");  
        datum.put("right", "R");  
  
  
        ByteArrayOutputStream out = new ByteArrayOutputStream();  
        //DatumWriter���Խ�GenericRecord���edncoder������������  
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);  
        //encoder���Խ�����д�����У�binaryEncoder�ڶ������������õ�encoder�����ﲻ���ã����ô���  
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);  
        writer.write(datum,encoder);  
        encoder.flush();  
        out.close();  
  
        DatumReader<GenericRecord> reader=new GenericDatumReader<GenericRecord>(schema);  
        Decoder decoder=DecoderFactory.get().binaryDecoder(out.toByteArray(),null);  
        GenericRecord result=reader.read(null,decoder);  
        Assert.assertEquals("L",result.get("left").toString());  
        Assert.assertEquals("R",result.get("right").toString());  
        
    }  

}
