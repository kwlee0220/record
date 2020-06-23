package record.optor.support;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import record.DefaultRecord;
import record.Record;
import record.RecordSchema;
import record.optor.RecordSetFunction;
import record.optor.RecordSetOperatorContext;
import utils.LoggerSettable;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractRecordSetFunction implements RecordSetFunction, LoggerSettable {
	protected RecordSetOperatorContext m_context;
	protected RecordSchema m_inputSchema;
	protected RecordSchema m_outputSchema;
	private Logger m_logger = LoggerFactory.getLogger(getClass());
	
	public final void checkInitialized() {
		if ( !isInitialized() ) {
			throw new IllegalStateException("not initialized: optor=" + this);
		}
	}
	
	public final boolean isInitialized() {
		return m_outputSchema != null;
	}

	@Override
	public final RecordSetOperatorContext getContext() {
		if ( m_context == null ) {
			throw new IllegalStateException("RecordSetOperatorContext is not present: optor=" + this);
		}
		
		return m_context;
	}
	
	@Override
	public final RecordSchema getInputRecordSchema() {
		checkInitialized();
		
		return m_inputSchema;
	}

	@Override
	public final RecordSchema getRecordSchema() {
		checkInitialized();
		
		return m_outputSchema;
	}
	
	protected final void setInitialized(@Nullable RecordSetOperatorContext context,
										RecordSchema inputSchema, RecordSchema outputSchema)	{
		Utilities.checkNotNullArgument(inputSchema, "inputSchema is null");
		Utilities.checkNotNullArgument(outputSchema, "outputSchema is null");
		
		m_context = context;
		m_inputSchema = inputSchema;
		m_outputSchema = outputSchema;
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
	
	protected final Record newInputRecord() {
		return DefaultRecord.of(m_inputSchema);
	}
}
