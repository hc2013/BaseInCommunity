package io.transwarp.geo.udf;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.hive.ql.exec.UDF;

public class WktToWkbUDF2 extends UDF {

  public byte[] convert(String wkt) {
    WKTReader wktReader = new WKTReader();
    Geometry geometry = parseGeometry(wkt, wktReader);
    if (geometry == null) {
      throw new IllegalArgumentException("empty geometry[" + wkt + "]");
    }

    WKBWriter wkbWriter = new WKBWriter();
    return wkbWriter.write(geometry);
  }

  private Geometry parseGeometry(String wkt, WKTReader wktReader) {
    try {
      return wktReader.read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException("failed to parse geometry[" + wkt + "]", e);
    }
  }

  public byte[] evaluate(String communityWktStr) {
    return convert(communityWktStr);
  }

}
