package record.type;

import com.vividsolutions.jts.geom.MultiLineString;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiLineStringType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	
	private static final MultiLineStringType TYPE = new MultiLineStringType();
	public final static MultiLineString EMPTY = GEOM_FACT.createMultiLineString(null);
	
	public static MultiLineStringType get() {
		return TYPE;
	}
	
	private MultiLineStringType() {
		super("multi_line_string", TypeCode.MULTI_LINESTRING, MultiLineString.class);
	}

	@Override
	public MultiLineString newInstance() {
		return EMPTY;
	}
}
