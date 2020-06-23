package record.type;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Point;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PointType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	public final static Point EMPTY = GEOM_FACT.createPoint((Coordinate)null);
	private static final PointType TYPE = new PointType();
	
	public static PointType get() {
		return TYPE;
	}
	
	private PointType() {
		super("point", TypeCode.POINT, Point.class);
	}

	@Override
	public Point newInstance() {
		return EMPTY;
	}
	
	public static Point toPoint(Coordinate coord) {
		return GEOM_FACT.createPoint(coord);
	}
}
