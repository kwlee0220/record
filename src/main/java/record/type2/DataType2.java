package record.type2;

import java.io.Serializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class DataType2 implements Serializable {
	private final String m_name;
	private final Class<?> m_instCls;
	
	/**
	 * 본 데이터 타입의 empty 데이터를 생성한다.
	 * 
	 * @return 데이터 객체.
	 */
	public abstract Object newInstance();
	
	/**
	 * 주어진 스트링 representation을 파싱하여 데이터 객체를 생성한다.
	 * 
	 * @param str	파싱할 대상 데이터 표현 스트림
	 * @return	데이터 객체
	 */
	public abstract Object parseString(String str);
	
	protected DataType2(String name, Class<?> instClass) {
		m_name = name;
		m_instCls = instClass;
	}

	public final String getName() {
		return m_name;
	}
	
	public final Class<?> getInstanceClass() {
		return m_instCls;
	}
	
	@Override
	public String toString() {
		return m_name;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || obj.getClass() != getClass() ) {
			return false;
		}
		
		DataType2 other = (DataType2)obj;
		return m_name.equals(other.m_name);
	}
	
	@Override
	public int hashCode() {
		return m_name.hashCode();
	}
}
