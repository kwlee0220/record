package record.optor;

import java.util.List;

import record.DataSet;
import record.Record;
import record.RecordSchema;
import record.RecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordListDataSet implements DataSet {
	private final RecordSchema m_schema;
	private final List<Record> m_records;
	
	public RecordListDataSet(RecordSchema schema, List<Record> records) {
		m_schema = schema;
		m_records = records;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public RecordStream read() {
		return RecordStream.from(m_schema, m_records);
	}
}
