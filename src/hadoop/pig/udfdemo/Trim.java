package hadoop.pig.udfdemo;

import java.io.IOException;

import org.apache.pig.PrimitiveEvalFunc;
/*
 *  �Զ�����㺯��
 */
public class Trim extends PrimitiveEvalFunc<String,String>{

	@Override
	public String exec(String input) throws IOException {
		//ȥ�������ַ�����ͷ�ͽ�β�Ŀհ׷�
		return input.trim();
	}
}
