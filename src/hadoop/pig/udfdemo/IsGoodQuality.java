package hadoop.pig.udfdemo;
/*
 * 这个函数主要是判断一个temperature的quality是否是有效
 */

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

//所有的过滤函数都是FilterFunc的子类
public class IsGoodQuality extends FilterFunc{

	@Override
	public Boolean exec(Tuple tuple) throws IOException {
		// TODO Auto-generated method stub
		if(tuple==null || tuple.size()==0){
			return false;
		}
		Object object = tuple.get(0);
		if(object==null){
			return false;
		}
		int i = (int) object;
		return i==0 || i==1 || i==4 || i==5 || i==9;
	}

}
