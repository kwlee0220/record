package record.optor;

import record.Record;
import record.RecordSchema;
import record.optor.support.RecordLevelTransform;
import record.optor.support.colexpr.ColumnSelector;
import record.optor.support.colexpr.ColumnSelectorFactory;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Project extends RecordLevelTransform {
	private final String m_columnSelection;
	
	private ColumnSelector m_selector;

	/**
	 * 주어진 컬럼 이름들로 구성된 projection 스트림 연산자를 생성한다.
	 * 연산 수행 결과로 생성된 레코드 세트는 입력 레코드 세트에 포함된 각 레코드들에 대해
	 * 주어진 이름의 컬럼만으로 구성된 레코드들로 구성된다. 
	 * 
	 * @param	columnSelection	projection 연산에 사용될 컬럼들의 이름 배열.
	 */
	public Project(String columnSelection) {
		Utilities.checkArgument(columnSelection != null, "Column seelection expression is null");
		
		m_columnSelection = columnSelection;
	}
	
	public Project(MultiColumnKey keys) {
		Utilities.checkNotNullArgument(keys, "keys is null");
		
		m_columnSelection = keys.streamKeyColumns()
								.map(KeyColumn::name)
								.join(",");
	}
	
	public String getColumnSelection() {
		return m_columnSelection;
	}

	@Override
	public void initialize(RecordSetOperatorContext context, RecordSchema inputSchema) {
		try {
			m_selector = ColumnSelectorFactory.create(inputSchema, m_columnSelection);
			
			setInitialized(context, inputSchema, m_selector.getRecordSchema());
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}

	@Override
	public boolean transform(Record input, Record output) {
		m_selector.select(input, output);
		
		return true;
	}
	
	@Override
	public String toString() {
		return String.format("%s: '%s'", getClass().getSimpleName(), m_columnSelection);
	}
}