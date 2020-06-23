package record.type;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeometryCollectionType extends GeometryDataType {
	private static final long serialVersionUID = 1L;
	private static final GeometryCollectionType TYPE = new GeometryCollectionType();
	public final static GeometryCollection EMPTY = GEOM_FACT.createGeometryCollection(new Geometry[0]);
	
	public static GeometryCollectionType get() {
		return TYPE;
	}
	
	private GeometryCollectionType() {
		super("geom_collection", TypeCode.GEOM_COLLECTION, GeometryCollection.class);
	}

	@Override
	public GeometryCollection newInstance() {
		return EMPTY;
	}
}
