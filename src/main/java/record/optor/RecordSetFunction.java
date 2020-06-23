package record.optor;

import java.util.function.Function;

import record.RecordSchema;
import record.RecordSet;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordSetFunction extends RecordSetOperator, Function<RecordSet, RecordSet> {
	/**
	 * 레코드세트 함수 초기화시 설정된 입력 레코드세트 스키마를 반환한다.
	 * 
	 * @return	입력 레코드세트 스키마
	 */
	public RecordSchema getInputRecordSchema();
	
	/**
	 * 주어진 입력 레코드세트를 이용하여 본 연산을 수행한 결과 레코드세트를 반환한다.
	 * 
	 * @param input		입력 레코드세트.
	 * @return	연산 수행결과로 생성된 레코드세트 객체.
	 */
	public RecordSet apply(RecordSet input);
}