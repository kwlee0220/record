package record.optor;

import java.util.function.Predicate;

import record.DataSet;
import record.Record;
import record.RecordSchema;
import record.RecordStream;
import record.stream.AbstractRecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FilteredDataSet implements DataSet {
	private final DataSet m_input;
	private final Predicate<? super Record> m_pred;
	
	public FilteredDataSet(DataSet input, Predicate<? super Record> pred) {
		m_input = input;
		m_pred = pred;
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_input.getRecordSchema();
	}

	@Override
	public RecordStream read() {
		return new StreamImpl(m_input.read(), m_pred);
	}

	static class StreamImpl extends AbstractRecordStream {
		private final RecordStream m_input;
		private final Predicate<? super Record> m_pred;
		
		StreamImpl(RecordStream input, Predicate<? super Record> pred) {
			m_input = input;
			m_pred = pred;
		}
		
		@Override
		public RecordSchema getRecordSchema() {
			return m_input.getRecordSchema();
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_input.close();
		}
		
		@Override
		public boolean next(Record output) {
			while ( next(output) ) {
				if ( m_pred.test(output) ) {
					return true;
				}
			}
			
			return false;
		}
		
		@Override
		public Record nextCopy() {
			Record rec;
			while ( (rec = nextCopy()) != null ) {
				if ( m_pred.test(rec) ) {
					return rec;
				}
			}
			
			return null;
		}
	}
}
