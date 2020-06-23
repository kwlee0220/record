package record.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum TypeCode {
	NULL(0),
	BYTE(1), SHORT(2), INT(3), LONG(4), FLOAT(5), DOUBLE(6), BOOLEAN(7), STRING(8), BINARY(9),
	
	DATE(10), TIME(11), DATETIME(12),
	
	COORDINATE(13), ENVELOPE(14),
	POINT(15), MULTI_POINT(16), LINESTRING(17), MULTI_LINESTRING(18), POLYGON(19),
		MULTI_POLYGON(20), GEOM_COLLECTION(21), GEOMETRY(22);
	
	private final byte m_code;
	
	private TypeCode(int code) {
		m_code = (byte)code;
	}
	
	public int get() {
		return m_code;
	}
	
	public static boolean isValid(int code) {
		return code > 0 && code <= GEOMETRY.m_code;
	}
	
	public static TypeCode fromCode(int code) {
		return values()[code];
	}
}