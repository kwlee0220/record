package record.type;

import java.io.IOException;
import java.io.InputStream;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.InputStreamInStream;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class GeometryDataType extends DataType {
	private static final long serialVersionUID = 1L;
	final static GeometryFactory GEOM_FACT = new GeometryFactory();
	final static Point EMPTY_POINT = GEOM_FACT.createPoint((Coordinate)null);

	public abstract Geometry newInstance();
	
	protected GeometryDataType(String name, TypeCode tc, Class<?> instClass) {
		super(name, tc, instClass);
	}
	
	public final GeometryDataType pluralType() {
		switch ( getTypeCode() ) {
			case POINT:
			case MULTI_POINT:
				return DataType.MULTI_POINT;
			case LINESTRING:
			case MULTI_LINESTRING:
				return DataType.MULTI_LINESTRING;
			case POLYGON:
			case MULTI_POLYGON:
				return DataType.MULTI_POLYGON;
			case GEOM_COLLECTION:
			case GEOMETRY:
				return DataType.GEOM_COLLECTION;
			default:
				throw new AssertionError();
		}
	}
	
	@Override
	public Geometry parseInstance(String wkt) {
		return fromWkt(wkt);
	}
	
	@Override
	public String toInstanceString(Object geom) {
		Utilities.checkNotNullArgument(geom, "input Geometryis null");
		Utilities.checkArgument(geom instanceof Geometry, "input is not Geometry");
		
		return toWkt((Geometry)geom);
	}
	
	public static Geometry fromWkt(String wkt) {
		Utilities.checkNotNullArgument(wkt, "input WKT is null");

		if ( wkt == null ) {
			return null;
		}
		else if ( wkt.length() == 0 ) {
			return GeometryType.EMPTY;
		}
		else {
			try {
				return new WKTReader(GEOM_FACT).read(wkt);
			}
			catch ( ParseException e ) {
				throw new IllegalArgumentException("invalid WKT: wkt=" + wkt);
			}
		}
	}
	
	public static String toWkt(Geometry geom) {
		Utilities.checkNotNullArgument(geom, "input Geometryis null");
		
		return (geom != null) ? new WKTWriter().write((Geometry)geom) : null;
	}
	
	public static Geometry fromWkb(InputStream is) throws IOException {
		if ( is == null ) {
			return null;
		}
		else {
			try {
				return new WKBReader(GEOM_FACT).read(new InputStreamInStream(is));
			}
			catch ( ParseException e ) {
				throw new IllegalArgumentException("invalid WKB");
			}
		}
	}
	
	public static Geometry fromWkb(byte[] wkb) {
		Utilities.checkNotNullArgument(wkb, "input WKB is null");

		if ( wkb == null ) {
			return null;
		}
		else if ( wkb.length == 0 ) {
			return GeometryType.EMPTY;
		}
		else {
			try {
				return new WKBReader(GEOM_FACT).read(wkb);
			}
			catch ( ParseException e ) {
				throw new IllegalArgumentException("invalid WKB");
			}
		}
	}
	
	public static byte[] toWkb(Geometry geom) {
		Utilities.checkNotNullArgument(geom, "input Geometryis null");
		Utilities.checkArgument(geom instanceof Geometry, "input is not Geometry");
		
		return (geom != null) ? new WKBWriter().write(geom) : null;
	}
	
	public static GeometryDataType fromGeometry(Geometry geom) {
		if ( geom instanceof Point ) {
			return DataType.POINT;
		}
		else if ( geom instanceof Polygon ) {
			return DataType.POLYGON;
		}
		else if ( geom instanceof MultiPolygon ) {
			return DataType.MULTI_POLYGON;
		}
		else if ( geom instanceof LineString ) {
			return DataType.LINESTRING;
		}
		else if ( geom instanceof MultiLineString ) {
			return DataType.MULTI_LINESTRING;
		}
		else if ( geom instanceof MultiPoint ) {
			return DataType.MULTI_POINT;
		}
		else if ( geom instanceof GeometryCollection ) {
			return DataType.GEOM_COLLECTION;
		}
		else {
			throw new AssertionError();
		}
	}
}
