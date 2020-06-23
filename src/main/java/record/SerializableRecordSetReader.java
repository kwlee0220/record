package record;

import java.io.IOException;
import java.io.ObjectInputStream;

import com.vividsolutions.jts.io.ParseException;

import record.support.SerializableUtils;
import record.type.DataType;
import utils.LocalDateTimes;
import utils.LocalDates;
import utils.func.CheckedSupplierX;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SerializableRecordSetReader implements RecordSetReader {
	private final RecordSchema m_schema;
	private final CheckedSupplierX<ObjectInputStream, IOException> m_inputSupplier;
	
	SerializableRecordSetReader(RecordSchema schema,
								CheckedSupplierX<ObjectInputStream, IOException> inputSupplier) {
		m_schema = schema;
		m_inputSupplier = inputSupplier;
	}

	@Override
	public RecordSet read() throws IOException {
		return new RecordSetImpl(m_inputSupplier.get());
	}
	
	private class RecordSetImpl extends AbstractRecordSet {
		private final ObjectInputStream m_dis;
		private boolean m_eos = false;
		
		RecordSetImpl(ObjectInputStream dis) {
			m_dis = dis;
		}
		
		@Override
		protected void closeInGuard() throws Exception {
			m_dis.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		@Override
		public boolean next(Record output) {
			try {
				if ( m_eos || m_dis.readByte() == 0 ) {
					m_eos = true;
					return false;
				}
				
				readRecord(m_dis, m_schema, output);
				return true;
			}
			catch ( IOException e ) {
				throw new RecordSetException("" + e);
			}
		}
	}
	
	private static void readRecord(ObjectInputStream in, RecordSchema schema, Record output) throws IOException {
		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			output.set(i, readColumn(in, schema.getColumnAt(i)));
		}
	}

	private static Object readColumn(ObjectInputStream in, Column col) throws IOException {
		DataType type = col.type();
		if ( type.isGeometryType() ) {
			try {
				return SerializableUtils.readGeometry(in);
			}
			catch ( ParseException e ) {
				throw new IOException("invalid WKB, cause=" + e);
			}
		}
		else {
			switch ( type.getTypeCode() ) {
				case STRING:
					return in.readUTF();
				case INT:
					return in.readInt();
				case LONG:
					return in.readLong();
				case FLOAT:
					return in.readFloat();
				case DOUBLE:
					return in.readDouble();
				case SHORT:
					return in.readShort();
				case BYTE:
					return in.readByte();
				case BINARY:
					return SerializableUtils.readBinary(in);
				case BOOLEAN:
					return in.readBoolean();
				case DATETIME:
					return LocalDateTimes.fromEpochMillis(in.readLong());
				case COORDINATE:
					return SerializableUtils.readCoordinate(in);
				case ENVELOPE:
					return SerializableUtils.readEnvelope(in);
				case DATE:
					return LocalDates.fromEpochMillis(in.readLong());
				default:
					throw new AssertionError("unexpected colum type: column=" + col);
			}
		}
	}

}
