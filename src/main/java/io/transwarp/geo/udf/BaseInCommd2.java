package io.transwarp.geo.udf;

import com.vividsolutions.jts.algorithm.locate.SimplePointInAreaLocator;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBReader;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import java.util.Arrays;

public class BaseInCommd2 extends UDF {

  private Polygon parsePolygon(WKBReader reader, byte[] wkbPolygon) {
    try {
      return (Polygon) reader.read(wkbPolygon);
    } catch (Exception e) {
      throw new IllegalArgumentException("failed to parse polygon[" + Arrays.toString(wkbPolygon) + "]", e);
    }
  }

  public boolean check(double longitude, double latitude, byte[] bytes) {
    WKBReader wkbReader = new WKBReader();
    Polygon polygon = parsePolygon(wkbReader, bytes);
    Coordinate coordinate = new Coordinate(longitude, latitude);
    return SimplePointInAreaLocator.containsPointInPolygon(coordinate, polygon);
  }

  public boolean evaluate(double lon, double lat, BytesWritable polygon) {
    if (polygon == null) {
      return false;
    }
    boolean result = check(lon, lat, polygon.getBytes());
    return result;
  }

}
