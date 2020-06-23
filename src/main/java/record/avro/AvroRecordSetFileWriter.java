package record.avro;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import record.RecordSet;
import record.RecordSetWriter;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroRecordSetFileWriter implements RecordSetWriter {
	private final File m_file;
	private final OutputStream m_os;
	private int m_syncInterval = (int)UnitUtils.parseByteSize("256kb");
	private CodecFactory m_codec = null;
	
	private Schema m_avroSchema;
	private DataFileWriter<GenericRecord> m_writer;
	
	AvroRecordSetFileWriter(File file) {
		m_file = file;
		m_os = null;
	}
	
	AvroRecordSetFileWriter(OutputStream os) {
		m_file = null;
		m_os = os;
	}
	
	public AvroRecordSetFileWriter setSyncInterval(int interval) {
		m_syncInterval = interval;
		return this;
	}
	
	public AvroRecordSetFileWriter setSyncInterval(String interval) {
		m_syncInterval = (int)UnitUtils.parseByteSize(interval);
		return this;
	}
	
	public AvroRecordSetFileWriter setCodec(CodecFactory fact) {
		m_codec = fact;
		return this;
	}

	@Override
	public void close() throws IOException {
		m_writer.close();
	}

	@Override
	public long write(RecordSet rset) throws IOException {
		if ( m_writer == null ) {
			m_avroSchema = AvroRecordAdaptors.toSchema(rset.getRecordSchema());
			GenericDatumWriter<GenericRecord> datumWriter
											= new GenericDatumWriter<GenericRecord>(m_avroSchema);
			m_writer = new DataFileWriter<>(datumWriter)
							.setMeta("marmot_schema", rset.getRecordSchema().toString());
			if ( m_syncInterval > 0 ) {
				m_writer = m_writer.setSyncInterval(m_syncInterval);
			}
			if ( m_codec != null ) {
				m_writer = m_writer.setCodec(m_codec);
			}
			if ( m_file != null ) {
				m_writer.create(m_avroSchema, m_file);
			}
			else {
				m_writer.create(m_avroSchema, m_os);
			}
		}
		
		long count = 0;
		AvroRecord output = new AvroRecord(rset.getRecordSchema(), m_avroSchema);
		while ( rset.next(output) ) {
			m_writer.append(output.getGenericRecord());
			++count;
		}
		m_writer.flush();
		
		return count;
	}

}
