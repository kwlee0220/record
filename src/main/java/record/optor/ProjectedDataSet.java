package record.optor;

import record.DataSet;
import record.DefaultRecord;
import record.Record;
import record.RecordSchema;
import record.RecordStream;
import record.optor.support.colexpr.ColumnSelector;
import record.optor.support.colexpr.ColumnSelectorFactory;
import record.stream.AbstractRecordStream;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ProjectedDataSet implements DataSet {
	private final DataSet m_input;
	private final String m_columnSelection;
	
	private final RecordSchema m_schema;
	private final ColumnSelector m_selector;

	/**
	 * 주어진 컬럼 이름들로 구성된 projection 스트림 연산자를 생성한다.
	 * 연산 수행 결과로 생성된 레코드 세트는 입력 레코드 세트에 포함된 각 레코드들에 대해
	 * 주어진 이름의 컬럼만으로 구성된 레코드들로 구성된다. 
	 * 
	 * @param	columnSelection	projection 연산에 사용될 컬럼들의 이름 배열.
	 */
	public ProjectedDataSet(DataSet input, String columnSelection) {
		Utilities.checkArgument(columnSelection != null, "Column seelection expression is null");

		m_input = input;
		m_columnSelection = columnSelection;
		m_selector = ColumnSelectorFactory.create(input.getRecordSchema(), m_columnSelection);
		m_schema = m_selector.getRecordSchema();
	}
	
	public ProjectedDataSet(DataSet input, MultiColumnKey keys) {
		this(input, keys.streamKeyColumns().map(KeyColumn::name).join(","));
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public String getColumnSelection() {
		return m_columnSelection;
	}

	@Override
	public RecordStream read() {
		return new IteratorImpl(m_input.read(), m_selector);
	}
	
	@Override
	public String toString() {
		return String.format("project: '%s'", getClass().getSimpleName(), m_columnSelection);
	}

	static class IteratorImpl extends AbstractRecordStream {
		private final RecordStream m_input;
		private final ColumnSelector m_selector;
		private final Record m_buffer;
		
		IteratorImpl(RecordStream input, ColumnSelector selector) {
			m_input = input;
			m_selector = selector;
			
			m_buffer = DefaultRecord.of(getRecordSchema());
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
			if ( !m_input.next(m_buffer) ) {
				return false;
			}
			else {
				m_selector.select(m_buffer, output);
				return true;
			}
		}
		
		@Override
		public Record nextCopy() {
			if ( !m_input.next(m_buffer) ) {
				return null;
			}
			
			Record output = DefaultRecord.of(getRecordSchema());
			m_selector.select(m_buffer, output);
			
			return output;
		}
	}
}