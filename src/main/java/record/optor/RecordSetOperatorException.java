package record.optor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSetOperatorException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public RecordSetOperatorException(String details) {
		super(details);
	}

	public RecordSetOperatorException(String details, Throwable cause) {
		super(details, cause);
	}
}
