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
 * 时间2018.01.19
 * 主要功能：Avro的通用映射（generic mapping）
 */

public class TestAvroGenericMapping {

	@Test
	public void test() throws IOException {  
        //将schema从StringPair.avsc文件中加载  
        Schema.Parser parser = new Schema.Parser();  
        Schema schema = parser.parse(getClass().getResourceAsStream("StringPair.avsc"));  
  
        //根据schema创建一个record示例  
        GenericRecord datum = new GenericData.Record(schema);  
        datum.put("left", "L");  
        datum.put("right", "R");  
  
  
        ByteArrayOutputStream out = new ByteArrayOutputStream();  
        //DatumWriter可以将GenericRecord变成edncoder可以理解的类型  
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);  
        //encoder可以将数据写入流中，binaryEncoder第二个参数是重用的encoder，这里不重用，所用传空  
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
