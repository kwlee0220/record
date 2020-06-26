package record.avro;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import record.DataSet;
import record.DataSetException;
import record.Record;
import record.RecordSchema;
import record.RecordStream;
import record.RecordStreamException;
import record.stream.AbstractRecordStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class AvroDataSet implements DataSet {
	private final Supplier<SeekableInput> m_supplier;
	private RecordSchema m_schema;
	
	AvroDataSet(Supplier<SeekableInput> supplier) {
		m_supplier = supplier;
	}

	@Override
	public RecordSchema getRecordSchema() {
		if ( m_schema == null ) {
			try {
				DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
				DataFileReader<GenericRecord> fileReader
												= new DataFileReader<>(m_supplier.get(), reader);
				m_schema = AvroDataSets.toRecordSchema(fileReader.getSchema());
			}
			catch ( IOException e ) {
				throw new RecordStreamException("" + e);
			}
		}
		
		return m_schema;
	}

	@Override
	public RecordStream read() {
		try {
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
			DataFileReader<GenericRecord> fileReader
												= new DataFileReader<>(m_supplier.get(), reader);
			if ( m_schema == null ) {
				m_schema = AvroDataSets.toRecordSchema(fileReader.getSchema());
			}
			return new StreamImpl(fileReader, m_schema);
		}
		catch ( IOException e ) {
			throw new DataSetException("" + e);
		}
	}

	private static class StreamImpl extends AbstractRecordStream {
		private final DataFileReader<GenericRecord> m_fileReader;
		private final RecordSchema m_schema;
		private final GenericRecord m_record;
		
		private StreamImpl(DataFileReader<GenericRecord> fileReader, RecordSchema schema) {
			m_fileReader = fileReader;
			m_schema = schema;
			m_record = new GenericData.Record(fileReader.getSchema());
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_fileReader.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public boolean next(Record output) {
			checkNotClosed();
			
			if ( !m_fileReader.hasNext() ) {
				return false;
			}
			
			try {
				if ( output instanceof AvroRecord ) {
					m_fileReader.next(((AvroRecord)output).getGenericRecord());
				}
				else {
					m_fileReader.next(m_record);
					for ( int i =0; i < m_schema.getColumnCount(); ++i ) {
						output.set(i, m_record.get(i));
					}
				}
				
				return true;
			}
			catch ( IOException e ) {
				throw new RecordStreamException("" + e);
			}
		}
		
		@Override
		public Record nextCopy() {
			checkNotClosed();
			
			if ( !m_fileReader.hasNext() ) {
				return null;
			}
			
			try {
				return new AvroRecord(m_schema, m_fileReader.next());
			}
			catch ( Exception e ) {
				throw new RecordStreamException("" + e);
			}
		}
	}
}
