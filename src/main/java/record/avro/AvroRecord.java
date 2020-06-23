package record.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import record.Record;
import record.RecordSchema;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class AvroRecord implements Record {
	private final RecordSchema m_schema;
	private final GenericRecord m_grecord;
	
	AvroRecord(RecordSchema schema, GenericRecord record) {
		m_schema = schema;
		m_grecord = record;
	}
	
	AvroRecord(RecordSchema schema, Schema avroSchema) {
		m_schema = schema;
		m_grecord = new GenericData.Record(avroSchema);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public GenericRecord getGenericRecord() {
		return m_grecord;
	}

	@Override
	public Object get(int index) {
		return m_grecord.get(index);
	}

	@Override
	public Record set(int idx, Object value) {
		Object field = AvroRecordAdaptors.toAvroValue(m_schema.getColumnAt(idx).type(), value);
		m_grecord.put(idx,  field);
		return this;
	}
}
