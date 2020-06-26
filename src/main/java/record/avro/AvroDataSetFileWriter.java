package record.avro;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import record.RecordStream;
import record.RecordStreamException;
import record.DataSetWriter;
import utils.UnitUtils;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AvroDataSetFileWriter implements DataSetWriter {
	private final File m_file;
	private final OutputStream m_os;
	private int m_syncInterval = (int)UnitUtils.parseByteSize("256kb");
	private CodecFactory m_codec = null;
	
	AvroDataSetFileWriter(File file) {
		m_file = file;
		m_os = null;
	}
	
	AvroDataSetFileWriter(OutputStream os) {
		m_file = null;
		m_os = os;
	}
	
	public AvroDataSetFileWriter setSyncInterval(int interval) {
		m_syncInterval = interval;
		return this;
	}
	
	public AvroDataSetFileWriter setSyncInterval(String interval) {
		m_syncInterval = (int)UnitUtils.parseByteSize(interval);
		return this;
	}
	
	public AvroDataSetFileWriter setCodec(CodecFactory fact) {
		m_codec = fact;
		return this;
	}

	@Override
	public long write(RecordStream stream) {
		Schema avroSchema = AvroDataSets.toSchema(stream.getRecordSchema());
		GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter);
		try {
			writer.setMeta("marmot_schema", stream.getRecordSchema().toString());
			if ( m_syncInterval > 0 ) {
				writer = writer.setSyncInterval(m_syncInterval);
			}
			if ( m_codec != null ) {
				writer = writer.setCodec(m_codec);
			}
			
			if ( m_file != null ) {
				writer.create(avroSchema, m_file);
			}
			else {
				writer.create(avroSchema, m_os);
			}
			
			long count = 0;
			AvroRecord output = new AvroRecord(stream.getRecordSchema(), avroSchema);
			while ( stream.next(output) ) {
				writer.append(output.getGenericRecord());
				++count;
			}
			writer.flush();
			
			return count;
		}
		catch ( IOException e ) {
			throw new RecordStreamException("" + e);
		}
		finally {
			IOUtils.closeQuietly(writer);
		}
	}
}
