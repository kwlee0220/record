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

import record.DataSet;
import record.Record;
import record.RecordSchema;
import record.RecordStream;
import record.RecordStreamException;
import record.stream.AbstractRecordStream;
import utils.func.CheckedSupplierX;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroBinaryDataSet implements DataSet {
	private static final DecoderFactory FACT = DecoderFactory.get();
	
	private final Schema m_avroSchema;
	private final RecordSchema m_schema;
	private final CheckedSupplierX<InputStream, IOException> m_inputGen;
	
	AvroBinaryDataSet(Schema avroSchema, File file) throws FileNotFoundException {
		m_avroSchema = avroSchema;
		m_schema = AvroDataSets.toRecordSchema(avroSchema);
		m_inputGen = () -> new BufferedInputStream(new FileInputStream(file));
	}
	
	AvroBinaryDataSet(Schema avroSchema, byte[] bytes) {
		m_avroSchema = avroSchema;
		m_schema = AvroDataSets.toRecordSchema(avroSchema);
		m_inputGen = () -> new ByteArrayInputStream(bytes);
	}
	
	AvroBinaryDataSet(Schema avroSchema, byte[] bytes, int offset, int length) {
		m_avroSchema = avroSchema;
		m_schema = AvroDataSets.toRecordSchema(avroSchema);
		m_inputGen = () -> new ByteArrayInputStream(bytes, offset, length);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public RecordStream read() {
		try {
			return new StreamImpl(m_avroSchema, m_inputGen.get());
		}
		catch ( IOException e ) {
			throw new RecordStreamException("" + e);
		}
	}
	
	private static class StreamImpl extends AbstractRecordStream {
		private final InputStream m_is;
		private final DatumReader<GenericRecord> m_reader;
		private final BinaryDecoder m_decoder;
		private final RecordSchema m_schema;
		private final GenericRecord m_record;
		
		private StreamImpl(Schema avroSchema, InputStream is) {
			m_is = is;
			m_reader = new GenericDatumReader<GenericRecord>();
			m_decoder = FACT.binaryDecoder(is, null);
			
			m_schema = AvroDataSets.toRecordSchema(avroSchema);
			m_record = new GenericData.Record(avroSchema);
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
				throw new RecordStreamException("" + e);
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
				throw new RecordStreamException("" + e);
			}
		}
	}
}
