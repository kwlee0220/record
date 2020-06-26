package record.optor;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.function.Supplier;

import record.DataSet;
import record.Record;
import record.RecordSchema;
import record.RecordStream;
import record.RecordStreamException;
import record.stream.AbstractRecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class SerializableDataSet implements DataSet {
	private final RecordSchema m_schema;
	private final Supplier<ObjectInputStream> m_inputSupplier;
	
	SerializableDataSet(RecordSchema schema, Supplier<ObjectInputStream> inputSupplier) {
		m_schema = schema;
		m_inputSupplier = inputSupplier;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(m_schema, m_inputSupplier.get());
	}
	
	private class StreamImpl extends AbstractRecordStream {
		private final RecordSchema m_schema;
		private final ObjectInputStream m_ois;
		private boolean m_hasNext;
		
		StreamImpl(RecordSchema schema, ObjectInputStream ois) {
			m_schema = schema;
			m_ois = ois;
		}
		
		@Override
		protected void closeInGuard() throws Exception {
			m_ois.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public boolean next(Record output) {
			try {
				if ( m_ois.readByte() == 0 ) {
					return false;
				}
				else {
					SerializableDataSets.readRecord(m_ois, m_schema, output);
					return true;
				}
			}
			catch ( IOException e ) {
				throw new RecordStreamException("" + e);
			}
		}
	}

}
