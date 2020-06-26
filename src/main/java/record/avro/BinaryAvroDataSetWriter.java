package record.avro;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import record.RecordStream;
import record.RecordStreamException;
import record.DataSetWriter;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinaryAvroDataSetWriter implements DataSetWriter {
	private final OutputStream m_os;
	
	private GenericDatumWriter<GenericRecord> m_datumWriter;
	private Encoder m_encoder;
	private Schema m_avroSchema;
	
	BinaryAvroDataSetWriter(OutputStream os) {
		m_os = os;
	}

	@Override
	public long write(RecordStream stream) {
		if ( m_datumWriter == null ) {
			m_avroSchema = AvroDataSets.toSchema(stream.getRecordSchema());
			m_datumWriter = new GenericDatumWriter<GenericRecord>(m_avroSchema);
			m_encoder = EncoderFactory.get().binaryEncoder(m_os, null);
		}
		
		try {
			long count = 0;
			AvroRecord output = new AvroRecord(stream.getRecordSchema(), m_avroSchema);
			while ( stream.next(output) ) {
				m_datumWriter.write(output.getGenericRecord(), m_encoder);
				++count;
			}
			m_encoder.flush();
			
			return count;
		}
		catch ( IOException e ) {
			throw new RecordStreamException("" + e);
		}
		finally {
			IOUtils.closeQuietly(m_os);
		}
	}

}
