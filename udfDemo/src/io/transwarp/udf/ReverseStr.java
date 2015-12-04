package io.transwarp.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * 普通UDF函数的实现需要继承UDF，然后实现evaluate方法。本例是将一个字符窜反转
 * @author sean
 *
 */
public class ReverseStr extends UDF {

	
	public String evaluate(String src){
		
		StringBuffer result=new StringBuffer();
		char[] charArr=src.trim().toCharArray();
		for(int i=charArr.length-1;i>=0;i--){
			result.append(charArr[i]);
		}
		return result.toString();
	}
}
