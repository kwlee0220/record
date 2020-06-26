package record.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import record.Column;
import record.DataSet;
import record.DataSetException;
import record.Record;
import record.RecordSchema;
import record.RecordStream;
import record.RecordStreamException;
import record.stream.AbstractRecordStream;
import utils.jdbc.JdbcProcessor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcDataSet implements DataSet {
	private final JdbcRecordAdaptor m_adaptor;
	private final String m_sql;
	
	public JdbcDataSet(JdbcRecordAdaptor adaptor, String tblName) {
		m_adaptor = adaptor;
		m_sql = adaptor.getRecordSchema()
						.streamColumns()
						.map(Column::name)
						.join(", ", "select ", " from " + tblName);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_adaptor.getRecordSchema();
	}

	@Override
	public RecordStream read() {
		try {
			JdbcProcessor jdbc = m_adaptor.getJdbcProcessor();
			return new StreamImpl(jdbc.executeQuery(m_sql));
		}
		catch ( SQLException e ) {
			throw new DataSetException("fails to execute query: " + m_sql + ", cause=" + e);
		}
	}

	private class StreamImpl extends AbstractRecordStream {
		private final ResultSet m_rs;
		
		StreamImpl(ResultSet rs) {
			m_rs = rs;
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_rs.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_adaptor.getRecordSchema();
		}
		
		@Override
		public boolean next(Record output) {
			try {
				if ( m_rs.next() ) {
					m_adaptor.loadRecord(m_rs, output);
					return true;
				}
				else {
					return false;
				}
			}
			catch ( SQLException e ) {
				throw new RecordStreamException("" + e);
			}
		}
	}
}
