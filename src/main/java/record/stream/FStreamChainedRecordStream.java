package record.stream;

import record.DataSet;
import record.RecordSchema;
import record.RecordStream;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FStreamChainedRecordStream extends ChainedRecordStream {
	private final FStream<? extends RecordStream> m_components;
	
	public static FStreamChainedRecordStream fromDataSets(RecordSchema schema,
														FStream<? extends DataSet> components) {
		return new FStreamChainedRecordStream(schema, components.map(DataSet::read));
	}
	
	public static FStreamChainedRecordStream from(RecordSchema schema,
													FStream<? extends RecordStream> components) {
		return new FStreamChainedRecordStream(schema, components);
	}
	
	public FStreamChainedRecordStream(RecordSchema schema,
									FStream<? extends RecordStream> components) {
		super(schema);
		
		Utilities.checkNotNullArgument(schema, "schema is null");
		Utilities.checkNotNullArgument(components, "components is null");
		
		m_components = components;
	}

	@Override
	protected RecordStream getNextRecordStream() {
		return m_components.next().getOrNull();
	}
}