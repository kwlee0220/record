package record.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import record.Column;
import record.Record;
import record.RecordSchema;
import record.RecordSet;
import record.RecordSetException;
import record.RecordSetWriter;
import utils.Utilities;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcRecordSetWriter implements RecordSetWriter {
	private final Logger s_logger = LoggerFactory.getLogger(JdbcRecordSetWriter.class);
	private static final int DEFAULT_BATCH_SIZE = 64;
	private static final int DISPLAY_GAP = 100000;
	
	private final String m_tblName;
	private final JdbcProcessor m_jdbc;
	private final JdbcRecordAdaptor m_adaptor;
	private int m_batchSize = DEFAULT_BATCH_SIZE;
	
	public JdbcRecordSetWriter(JdbcProcessor jdbc, String tblName, JdbcRecordAdaptor adaptor) {
		m_tblName = tblName;
		m_jdbc = jdbc;
		m_adaptor = adaptor;
	}

	@Override
	public void close() throws IOException {
	}
	
	public JdbcRecordSetWriter setBatchSize(int size) {
		m_batchSize = size;
		return this;
	}

	@Override
	public long write(RecordSet rset) throws IOException {
		Utilities.checkNotNullArgument(rset, "rset is null");
		
		String valuesExpr = createInsertValueExpr(m_adaptor);
		String insertStmtStr = String.format("insert into %s %s", m_tblName, valuesExpr);

		AtomicInteger count = new AtomicInteger(0);
		try ( Connection conn = m_jdbc.connect() ) {
			PreparedStatement pstmt = conn.prepareStatement(insertStmtStr);
			s_logger.info("connected: {}", this);
			
			Record record;
			while ( (record = rset.nextCopy()) != null ) {
				try {
					m_adaptor.storeRecord(record, pstmt);
					pstmt.addBatch();
					
					if ( count.incrementAndGet() % m_batchSize == 0 ) {
						pstmt.executeBatch();
						s_logger.debug("inserted: {} records", count);
					}

					if ( count.get() % DISPLAY_GAP == 0 ) {
						s_logger.info("inserted: {} records", count);
					}
				}
				catch ( SQLException e ) {
					throw new RecordSetException(String.format("fails to store output: jdbc=%s, table=%s, cause=%s",
																m_jdbc, m_tblName, e));
				}
			}
			pstmt.executeBatch();
		}
		catch ( RecordSetException e ) {
			throw e;
		}
		catch ( SQLException e ) {
			System.out.println("count=" + count.get());
			throw new RecordSetException(e);
		}
		s_logger.info("inserted: {} records", count.get());
		
		return count.get();
	}

	@Override
	public String toString() {
		return String.format("%s, tblname=%s", m_jdbc, m_tblName);
	}
	
	private String createInsertValueExpr(JdbcRecordAdaptor adaptor) {
		RecordSchema schema = adaptor.getRecordSchema();
		
		String colListExpr = schema.streamColumns().map(Column::name).join(",", "(", ")");
		String colValExpr = schema.streamColumns()
									.map(adaptor::getInsertValueExpr)
									.join(",", "(", ")");
		return colListExpr + " values " + colValExpr;
	}
}
