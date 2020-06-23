package record;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSetException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public RecordSetException(String details) {
		super(details);
	}

	public RecordSetException(Throwable cause) {
		super(cause);
	}

	public RecordSetException(String details, Throwable cause) {
		super(details, cause);
	}
}
