package record.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NullType extends DataType {
	private static final long serialVersionUID = 1L;
	private static final NullType TYPE = new NullType();
	
	private NullType() {
		super("null", TypeCode.NULL, Void.class);
	}
	
	public static NullType get() {
		return TYPE;
	}
	
	@Override
	public Byte newInstance() {
		throw new AssertionError("should not be called");
	}
	
	@Override
	public Byte parseInstance(String str) {
		throw new AssertionError("should not be called");
	}
}
