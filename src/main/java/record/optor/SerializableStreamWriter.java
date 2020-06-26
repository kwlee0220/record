package record.optor;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import record.Column;
import record.DataSetException;
import record.DefaultRecord;
import record.Record;
import record.RecordSchema;
import record.RecordStream;
import record.DataSetWriter;
import record.support.SerializableUtils;
import record.type.DataType;
import utils.LocalDateTimes;
import utils.LocalDates;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SerializableStreamWriter implements DataSetWriter {
	private final ObjectOutputStream m_dos;
	
	public SerializableStreamWriter(ObjectOutputStream dos) {
		m_dos = dos;
	}

	@Override
	public long write(RecordStream stream) {
		RecordSchema schema = stream.getRecordSchema();
		
		try {
			long count = 0;
			Record rec = DefaultRecord.of(schema);
			while ( stream.next(rec) ) {
				m_dos.writeByte(1);
				for ( int i =0; i < schema.getColumnCount(); ++i ) {
					writeColumn(schema.getColumnAt(i), rec.get(i));
				}
				++count;
			}
			m_dos.writeByte(0);
			
			return count;
		}
		catch ( IOException e ) {
			throw new DataSetException("" + e);
		}
	}

	private void writeColumn(Column col, Object value) throws IOException {
		DataType type = col.type();
		if ( type.isGeometryType() ) {
			SerializableUtils.writeGeometry(m_dos, (Geometry)value);
		}
		else {
			switch ( type.getTypeCode() ) {
				case STRING:
					m_dos.writeUTF((String)value);
					break;
				case INT:
					m_dos.writeInt((Integer)value);
					break;
				case LONG:
					m_dos.writeLong((Long)value);
					break;
				case FLOAT:
					m_dos.writeFloat((Float)value);
					break;
				case DOUBLE:
					m_dos.writeDouble((Double)value);
					break;
				case SHORT:
					m_dos.writeShort((Short)value);
					break;
				case BYTE:
					m_dos.writeByte((byte)value);
					break;
				case BINARY:
					SerializableUtils.writeBinary(m_dos, (byte[])value);
					break;
				case BOOLEAN:
					m_dos.writeBoolean((Boolean)value);
					break;
				case DATETIME:
					m_dos.writeLong(LocalDateTimes.toEpochMillis((LocalDateTime)value));
					break;
				case COORDINATE:
					SerializableUtils.writeCoordinate(m_dos, (Coordinate)value);
					break;
				case ENVELOPE:
					SerializableUtils.writeEnvelope(m_dos, (Envelope)value);
					break;
				case DATE:
					m_dos.writeLong(LocalDates.toEpochMillis((LocalDate)value));
					break;
				default:
					throw new AssertionError("unexpected colum type: column=" + col);
			}
		}
	}
}
