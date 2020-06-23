package record.avro;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import record.RecordSet;
import record.RecordSetWriter;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinaryAvroRecordSetWriter implements RecordSetWriter {
	private final OutputStream m_os;
	
	private GenericDatumWriter<GenericRecord> m_datumWriter;
	private Encoder m_encoder;
	private Schema m_avroSchema;
	
	BinaryAvroRecordSetWriter(OutputStream os) {
		m_os = os;
	}

	@Override
	public void close() throws IOException {
		m_os.close();
	}

	@Override
	public long write(RecordSet rset) throws IOException {
		if ( m_datumWriter == null ) {
			m_avroSchema = AvroRecordAdaptors.toSchema(rset.getRecordSchema());
			m_datumWriter = new GenericDatumWriter<GenericRecord>(m_avroSchema);
			m_encoder = EncoderFactory.get().binaryEncoder(m_os, null);
		}
		
		long count = 0;
		AvroRecord output = new AvroRecord(rset.getRecordSchema(), m_avroSchema);
		while ( rset.next(output) ) {
			m_datumWriter.write(output.getGenericRecord(), m_encoder);
			++count;
		}
		m_encoder.flush();
		
		return count;
	}

}
