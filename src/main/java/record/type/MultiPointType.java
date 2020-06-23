package record.type;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.MultiPoint;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiPointType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	private static final MultiPointType TYPE = new MultiPointType();
	public final static MultiPoint EMPTY = GEOM_FACT.createMultiPoint((Coordinate[])null);
	
	public static MultiPointType get() {
		return TYPE;
	}
	
	private MultiPointType() {
		super("multi_point", TypeCode.MULTI_POINT, MultiPoint.class);
	}

	@Override
	public MultiPoint newInstance() {
		return EMPTY;
	}
}
