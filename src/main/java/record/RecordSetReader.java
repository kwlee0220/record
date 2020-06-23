package record;

import java.io.IOException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordSetReader {
	/**
	 * RecordSet 객체를 반환한다.
	 * 
	 * @return	{@link RecordSet} 객체.
	 * @throws IOException RecordSet 객체 생성 중 입출력 예외가 발생된 경우.
	 */
	public RecordSet read() throws IOException;
}
