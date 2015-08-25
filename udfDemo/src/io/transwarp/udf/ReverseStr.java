package io.transwarp.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

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
