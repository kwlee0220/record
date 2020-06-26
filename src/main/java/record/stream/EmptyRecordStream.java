package record.stream;

import record.Record;
import record.RecordSchema;
import record.RecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EmptyRecordStream implements RecordStream {
	private final RecordSchema m_schema;
	
	public EmptyRecordStream(RecordSchema schema) {
		m_schema = schema;
	}
	
	@Override
	public void close() { }

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public boolean next(Record output) {
		return false;
	}

	@Override
	public Record nextCopy() {
		return null;
	}
}
