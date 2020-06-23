package record.type;

import com.vividsolutions.jts.geom.MultiPolygon;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiPolygonType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	private static final MultiPolygonType TYPE = new MultiPolygonType();
	public final static MultiPolygon EMPTY = GEOM_FACT.createMultiPolygon(null);
	
	public static MultiPolygonType get() {
		return TYPE;
	}
	
	private MultiPolygonType() {
		super("multi_polygon", TypeCode.MULTI_POLYGON, MultiPolygon.class);
	}

	@Override
	public MultiPolygon newInstance() {
		return EMPTY;
	}
}
