package record.type;

import com.vividsolutions.jts.geom.Geometry;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeometryType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	private static final GeometryType TYPE = new GeometryType();
	public final static Geometry EMPTY = GEOM_FACT.createGeometry(PointType.EMPTY);
	
	public static GeometryType get() {
		return TYPE;
	}
	
	private GeometryType() {
		super("geometry", TypeCode.GEOMETRY, Geometry.class);
	}

	@Override
	public Geometry newInstance() {
		return EMPTY;
	}
}
