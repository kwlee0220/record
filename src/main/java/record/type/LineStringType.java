package record.type;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LineStringType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	private static final LineStringType TYPE = new LineStringType();
	public final static LineString EMPTY = GEOM_FACT.createLineString(new Coordinate[0]);
	
	public static LineStringType get() {
		return TYPE;
	}
	
	private LineStringType() {
		super("line_string", TypeCode.LINESTRING, LineString.class);
	}

	@Override
	public LineString newInstance() {
		return EMPTY;
	}
}
