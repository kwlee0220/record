package record.optor;

import record.RecordSchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordSetOperator {
	public RecordSetOperatorContext getContext();
	
	/**
	 * 본 연산자를 통해 생성될 레코드 스트림의 스키마를 반환한다.
	 * 
	 * @return	레코드 스트림 스키마.
	 */
	public RecordSchema getRecordSchema();
}