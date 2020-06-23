package record;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSetClosedException extends RecordSetException {
	private static final long serialVersionUID = 1L;

	public RecordSetClosedException(String details) {
		super(details);
	}
}
