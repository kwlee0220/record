package record;

import java.io.Closeable;
import java.io.IOException;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordSetWriter extends Closeable {
	/**
	 * 레코드 세트에 포함된 모든 레코드를 저장한다.
	 * 
	 * @param rset	저장할 레코드를 포함한 레코드 세트 객체.
	 * @return	저장된 레코드의 갯수.
	 * @throws IOException	저장 도중 예외가 발생된 경우.
	 */
	public long write(RecordSet rset) throws IOException;
}
