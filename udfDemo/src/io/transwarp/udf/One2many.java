package io.transwarp.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
/**
 * 
 * 自定义UDTF函数需要继承GenericUDTF，然后重载initialize和process方法，本例将一个字符串按传入的分隔符切分
 * @author sean
 *
 */
public class One2many extends GenericUDTF {

	Object[] result = new Object[1];

	@Override
	public void close() throws HiveException {
	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		if (args.length != 2) {
			throw new UDFArgumentLengthException("One2many takes two argument");
		}

		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldNames.add("col1");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);
	}

	/**
	 * args[0]是带切分的字符串，args[1]是分隔符
	 */
	@Override
	public void process(Object[] args) throws HiveException {
		try {

			String record = ((LazyString) args[0]).toString();
			String delimiter = ((Text) args[1]).toString();
			String[] records = record.split(delimiter);
			for (int i = 0; i < records.length; i++) {
				result[0] = records[i];
				forward(result);
			}
			// int n = Integer.parseInt(args[0].toString());
			// for (int i = 0; i < n; i++) {
			// result[0] = i + 1;
			// forward(result);
			// }
		} catch (Exception e) {
			throw new HiveException(e.getMessage() + "---"
					+ args[0].getClass().getName() + "----"
					+ args[1].getClass().getName(), e);
		}
	}
}
