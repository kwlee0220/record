package record.type;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Polygon;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PolygonType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	private static final PolygonType TYPE = new PolygonType();
	public final static Polygon EMPTY = GEOM_FACT.createPolygon(new Coordinate[0]);
	
	public static PolygonType get() {
		return TYPE;
	}
	
	private PolygonType() {
		super("polygon", TypeCode.POLYGON, Polygon.class);
	}

	@Override
	public Polygon newInstance() {
		return EMPTY;
	}
}
