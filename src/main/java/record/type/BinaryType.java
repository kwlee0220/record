package record.type;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinaryType extends DataType {
	private static final long serialVersionUID = 1L;
	private static final BinaryType TYPE = new BinaryType();
	
	public static BinaryType get() {
		return TYPE;
	}
	
	private BinaryType() {
		super("binary", TypeCode.BINARY, byte[].class);
	}

	@Override
	public byte[] newInstance() {
		return new byte[0];
	}
	
	@Override
	public Object parseInstance(String str) {
		throw new UnsupportedOperationException();
	}
}
