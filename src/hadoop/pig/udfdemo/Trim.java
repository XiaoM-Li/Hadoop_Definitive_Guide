package hadoop.pig.udfdemo;

import java.io.IOException;

import org.apache.pig.PrimitiveEvalFunc;
/*
 *  自定义计算函数
 */
public class Trim extends PrimitiveEvalFunc<String,String>{

	@Override
	public String exec(String input) throws IOException {
		//去除输入字符串开头和结尾的空白符
		return input.trim();
	}
}
