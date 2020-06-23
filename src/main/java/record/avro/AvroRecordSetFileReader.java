package record.avro;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import record.AbstractRecordSet;
import record.Record;
import record.RecordSchema;
import record.RecordSet;
import record.RecordSetException;
import record.RecordSetReader;
import utils.func.CheckedSupplierX;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroRecordSetFileReader implements RecordSetReader {
	private final CheckedSupplierX<SeekableInput, IOException> m_inputGen;
	
	AvroRecordSetFileReader(File file) throws FileNotFoundException {
		m_inputGen = () -> new SeekableFileInput(file);
	}
	
	AvroRecordSetFileReader(byte[] bytes) {
		m_inputGen = () -> new SeekableByteArrayInput(bytes);
	}

	@Override
	public RecordSet read() throws IOException {
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> fileReader
								= new DataFileReader<GenericRecord>(m_inputGen.get(), reader);
		return new RecordSetImpl(fileReader);
	}

	private class RecordSetImpl extends AbstractRecordSet {
		private final DataFileReader<GenericRecord> m_fileReader;
		private final RecordSchema m_schema;
		private final GenericRecord m_record;
		
		private RecordSetImpl(DataFileReader<GenericRecord> fileReader) {
			m_fileReader = fileReader;
			Schema avroSchema = fileReader.getSchema();
			m_schema = AvroRecordAdaptors.toRecordSchema(avroSchema);
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
				throw new RecordSetException("" + e);
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
				throw new RecordSetException("" + e);
			}
		}
	}
}
