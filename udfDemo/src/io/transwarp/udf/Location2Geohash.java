package io.transwarp.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * 根据经纬度生成geohash
 * @author sean
 *
 */
public class Location2Geohash extends UDF {

	public String evaluate(double lat, double lon) throws IllegalArgumentException{
		String geohash = null;
		if (lat < -90 || lat > 90) {
			throw new IllegalArgumentException("latitude = "+lat+", latitude  must between -90 and 90.");
		}
		if (lon < -180 || lon > 180) {
			throw new IllegalArgumentException("longitude ="+lon+", longitude   must between -180 and 180.");
		}
		geohash = Geohash.encode(lat, lon);

		return geohash;
	}

}
