package record.optor;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import record.Column;
import record.DataSet;
import record.DataSetException;
import record.Record;
import record.RecordSchema;
import record.support.SerializableUtils;
import record.type.DataType;
import utils.LocalDateTimes;
import utils.LocalDates;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SerializableDataSets {
	private SerializableDataSets() {
		throw new AssertionError("should not be called: class=" + getClass());
	}
	
	public static DataSet get(RecordSchema schema, File file) {
		return new SerializableDataSet(schema, () -> {
			try {
				return new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
			}
			catch ( IOException e ) {
				throw new DataSetException("fails to read SerializableDataSet: path=" + file + ", cause=" + e);
			}
		});
	}
	public static DataSet get(RecordSchema schema, byte[] bytes) {
		return new SerializableDataSet(schema, () -> {
			try {
				return new ObjectInputStream(new ByteArrayInputStream(bytes));
			}
			catch ( IOException e ) {
				throw new DataSetException("fails to read SerializableDataSet: cause=" + e);
			}
		});
	}
	public static DataSet get(RecordSchema schema, byte[] bytes, int offset, int length) {
		return new SerializableDataSet(schema, () -> {
			try {
				return new ObjectInputStream(new ByteArrayInputStream(bytes, offset, length));
			}
			catch ( IOException e ) {
				throw new DataSetException("fails to read SerializableDataSet: cause=" + e);
			}
		});
	}
	
	static void readRecord(ObjectInputStream in, RecordSchema schema, Record output)
		throws IOException {
		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			output.set(i, readColumn(in, schema.getColumnAt(i)));
		}
	}

	static Object readColumn(ObjectInputStream in, Column col) throws IOException {
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

	static void writeColumn(ObjectOutputStream oos, Column col, Object value) throws IOException {
		DataType type = col.type();
		if ( type.isGeometryType() ) {
			SerializableUtils.writeGeometry(oos, (Geometry)value);
		}
		else {
			switch ( type.getTypeCode() ) {
				case STRING:
					oos.writeUTF((String)value);
					break;
				case INT:
					oos.writeInt((Integer)value);
					break;
				case LONG:
					oos.writeLong((Long)value);
					break;
				case FLOAT:
					oos.writeFloat((Float)value);
					break;
				case DOUBLE:
					oos.writeDouble((Double)value);
					break;
				case SHORT:
					oos.writeShort((Short)value);
					break;
				case BYTE:
					oos.writeByte((byte)value);
					break;
				case BINARY:
					SerializableUtils.writeBinary(oos, (byte[])value);
					break;
				case BOOLEAN:
					oos.writeBoolean((Boolean)value);
					break;
				case DATETIME:
					oos.writeLong(LocalDateTimes.toEpochMillis((LocalDateTime)value));
					break;
				case COORDINATE:
					SerializableUtils.writeCoordinate(oos, (Coordinate)value);
					break;
				case ENVELOPE:
					SerializableUtils.writeEnvelope(oos, (Envelope)value);
					break;
				case DATE:
					oos.writeLong(LocalDates.toEpochMillis((LocalDate)value));
					break;
				default:
					throw new AssertionError("unexpected colum type: column=" + col);
			}
		}
	}
}
