package record;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSetTimeoutException extends RecordSetException {
	private static final long serialVersionUID = 1L;

	public RecordSetTimeoutException(String details) {
		super(details);
	}
}
