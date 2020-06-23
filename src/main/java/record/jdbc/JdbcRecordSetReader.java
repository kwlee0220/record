package record.jdbc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import record.AbstractRecordSet;
import record.Column;
import record.Record;
import record.RecordSchema;
import record.RecordSet;
import record.RecordSetException;
import record.RecordSetReader;
import utils.jdbc.JdbcProcessor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcRecordSetReader implements RecordSetReader {
	private final JdbcRecordAdaptor m_adaptor;
	private final String m_sql;
	
	public JdbcRecordSetReader(JdbcRecordAdaptor adaptor, String tblName) {
		m_adaptor = adaptor;
		
		m_sql = adaptor.getRecordSchema()
						.streamColumns()
						.map(Column::name)
						.join(", ", "select ", " from " + tblName);
	}

	@Override
	public RecordSet read() throws IOException {
		try {
			JdbcProcessor jdbc = m_adaptor.getJdbcProcessor();
			return new RecordSetImpl(jdbc.executeQuery(m_sql));
		}
		catch ( SQLException e ) {
			throw new RecordSetException("fails to execute query: " + m_sql + ", cause=" + e);
		}
	}

	private class RecordSetImpl extends AbstractRecordSet {
		private final ResultSet m_rs;
		
		RecordSetImpl(ResultSet rs) {
			m_rs = rs;
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_adaptor.getRecordSchema();
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_rs.close();
		}
		
		@Override
		public boolean next(Record output) {
			try {
				if ( !m_rs.next() ) {
					return false;
				}
				
				m_adaptor.loadRecord(m_rs, output);
				return true;
			}
			catch ( SQLException e ) {
				throw new RecordSetException("" + e);
			}
		}
	}
}
