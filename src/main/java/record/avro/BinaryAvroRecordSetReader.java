package record.avro;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

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
public class BinaryAvroRecordSetReader implements RecordSetReader {
	private static final DecoderFactory FACT = DecoderFactory.get();
	
	private final Schema m_avroSchema;
	private final CheckedSupplierX<InputStream, IOException> m_inputGen;
	
	BinaryAvroRecordSetReader(Schema avroSchema, File file) throws FileNotFoundException {
		m_avroSchema = avroSchema;
		m_inputGen = () -> new BufferedInputStream(new FileInputStream(file));
	}
	
	BinaryAvroRecordSetReader(Schema avroSchema, byte[] bytes) {
		m_avroSchema = avroSchema;
		m_inputGen = () -> new ByteArrayInputStream(bytes);
	}
	
	BinaryAvroRecordSetReader(Schema avroSchema, byte[] bytes, int offset, int length) {
		m_avroSchema = avroSchema;
		m_inputGen = () -> new ByteArrayInputStream(bytes, offset, length);
	}

	@Override
	public RecordSet read() throws IOException {
		return new RecordSetImpl(m_inputGen.get());
	}
	
	private class RecordSetImpl extends AbstractRecordSet {
		private final InputStream m_is;
		private final DatumReader<GenericRecord> m_reader;
		private final BinaryDecoder m_decoder;
		private final RecordSchema m_schema;
		private final GenericRecord m_record;
		
		private RecordSetImpl(InputStream is) throws IOException {
			m_is = is;
			m_reader = new GenericDatumReader<GenericRecord>();
			m_decoder = FACT.binaryDecoder(is, null);
			
			m_schema = AvroRecordAdaptors.toRecordSchema(m_avroSchema);
			m_record = new GenericData.Record(m_avroSchema);
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_is.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public boolean next(Record output) {
			checkNotClosed();
			
			try {
				if ( m_decoder.isEnd() ) {
					return false;
				}
				
				if ( output instanceof AvroRecord ) {
					m_reader.read(((AvroRecord)output).getGenericRecord(), m_decoder);
				}
				else {
					m_reader.read(m_record, m_decoder);
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
			
			try {
				if ( m_decoder.isEnd() ) {
					return null;
				}
				
				return new AvroRecord(m_schema, m_reader.read(null, m_decoder));
			}
			catch ( Exception e ) {
				throw new RecordSetException("" + e);
			}
		}
	}

}
