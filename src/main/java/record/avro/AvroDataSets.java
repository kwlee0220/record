package record.avro;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.Date;
import org.apache.avro.LogicalTypes.TimeMillis;
import org.apache.avro.LogicalTypes.TimestampMillis;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Geometry;

import record.Column;
import record.DataSet;
import record.DataSetException;
import record.Record;
import record.RecordSchema;
import record.DataSetWriter;
import record.type.DataType;
import record.type.GeometryDataType;
import record.type.TypeCode;
import utils.LocalDateTimes;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class AvroDataSets {
	private AvroDataSets() {
		throw new AssertionError("should not be called: class=" + getClass());
	}
	
	public static DataSet fromFile(File file) {
		Supplier<SeekableInput> supplier = () -> {
			try {
				return new SeekableFileInput(file);
			}
			catch ( IOException e ) {
				throw new DataSetException("fails to read file: path=" + file);
			}
		};
		return new AvroDataSet(supplier);
	}
	
	public static DataSet fromBytes(byte[] bytes) {
		Supplier<SeekableInput> supplier = () -> new SeekableByteArrayInput(bytes);
		return new AvroDataSet(supplier);
	}
	
	public static DataSet fromRawFile(Schema schema, File file) throws FileNotFoundException {
		return new AvroBinaryDataSet(schema, file);
	}
	
	public static DataSet fromRawBytes(Schema schema, byte[] bytes) {
		return new AvroBinaryDataSet(schema, bytes);
	}
	
	public static DataSet fromRawBytes(Schema schema, byte[] bytes, int offset, int length) {
		return new AvroBinaryDataSet(schema, bytes, offset, length);
	}
	
	public static AvroDataSetFileWriter writer(File file) {
		return new AvroDataSetFileWriter(file);
	}
	
	public static AvroDataSetFileWriter writer(OutputStream os) {
		return new AvroDataSetFileWriter(os);
	}
	
	public static DataSetWriter binaryWriter(File file) throws FileNotFoundException {
		return binaryWriter(new BufferedOutputStream(new FileOutputStream(file), 4*1024));
	}
	
	public static DataSetWriter binaryWriter(OutputStream os) {
		return new BinaryAvroDataSetWriter(os);
	}
	
	public static Schema toSchema(final RecordSchema schema) {
		Utilities.checkNotNullArgument(schema);
		
		List<Field> fields = Lists.newArrayList();
		for ( Column col: schema.streamColumns() ) {
			Schema colSchema = PRIMITIVES.get(col.type().getTypeCode());
			if ( colSchema == null ) {
				throw new IllegalArgumentException("unsupported field type: column=" + col);
			}
			
			fields.add(new Field(col.name(), colSchema));
		}
		
		return Schema.createRecord("simple_feature", null, "etri.marmot", false, fields);
	}
	
	public static RecordSchema toRecordSchema(Schema schema) {
		return FStream.from(schema.getFields())
						.map(field -> new Column(field.name(), toColumnDataType(field.schema())))
						.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
						.build();
	}
	
	private static DataType toColumnDataType(Schema schema) {
		switch ( schema.getType() ) {
			case STRING:
				return DataType.STRING;
			case DOUBLE:
				return DataType.DOUBLE;
			case INT:
				return DataType.INT;
			case LONG:
				LogicalType ltype = schema.getLogicalType();
				if ( ltype != null ) {
					if ( ltype instanceof TimestampMillis ) {
						return DataType.DATETIME;
					}
					else if ( ltype instanceof Date ) {
						return DataType.DATE;
					}
					else if ( ltype instanceof TimeMillis ) {
						return DataType.TIME;
					}
					else {
						throw new IllegalArgumentException("unexpected field type: " + schema);
					}
				}
				else {
					return DataType.LONG;
				}
			case RECORD:
				List<Field> fields = schema.getFields();
				if ( fields.size() == 1 ) {
					DataType schemaType = DataType.fromName(schema.getName());
					if ( schemaType.isGeometryType() ) {
						return schemaType;
					}
				}
				throw new IllegalArgumentException("unexpected field type: " + schema);
			case BOOLEAN:
				return DataType.BOOLEAN;
			case FLOAT:
				return DataType.FLOAT;
			case BYTES:
				return DataType.BINARY;
			default:
				throw new IllegalArgumentException("unexpected field type: " + schema);
		}
	}
	
	public static Record toRecord(GenericRecord grec, RecordSchema schema) {
		return new AvroRecord(schema, grec);
	}
	
	public static GenericRecord toGenericRecord(Record src, Schema avroSchema) {
		if ( src instanceof AvroRecord ) {
			return ((AvroRecord)src).getGenericRecord();
		}
		
		RecordSchema rschema = src.getRecordSchema();
		GenericData.Record grec = new GenericData.Record(avroSchema);
		for ( int i =0; i < rschema.getColumnCount(); ++i ) {
			Object avroObj = toAvroValue(rschema.getColumnAt(i).type(), src.get(i));
			grec.put(i, avroObj);
		}
		
		return grec;
	}
	
	static Object toAvroValue(DataType type, Object value) {
		if ( type.isGeometryType() ) {
			Schema geomSchema = PRIMITIVES.get(type.getTypeCode());
			GenericRecord rec = new GenericData.Record(geomSchema);
			byte[] wkb = GeometryDataType.toWkb((Geometry)value);
			rec.put(0, ByteBuffer.wrap(wkb));
			
			return rec;
		}
		
		switch ( type.getTypeCode() ) {
			case STRING:
			case INT:
			case DOUBLE:
			case FLOAT:
			case LONG:
			case SHORT:
			case BYTE:
			case BINARY:
			case BOOLEAN:
				return value;
			case ENVELOPE:
				throw new AssertionError();
			case COORDINATE:
				throw new AssertionError();
			case DATETIME:
				return LocalDateTimes.fromEpochMillis((Long)value);
			default: 
				throw new AssertionError();
		}
	}
	
	static Object fromAvroValue(DataType type, Object value) {
		if ( type.isGeometryType() ) {
			return GeometryDataType.fromWkb((byte[])value);
		}
		
		switch ( type.getTypeCode() ) {
			case STRING:
			case INT:
			case DOUBLE:
			case FLOAT:
			case LONG:
			case SHORT:
			case BYTE:
			case BINARY:
			case BOOLEAN:
				return value;
			case ENVELOPE:
				throw new AssertionError();
			case COORDINATE:
				throw new AssertionError();
			case DATETIME:
				return LocalDateTimes.fromEpochMillis((Long)value);
			default: 
				throw new AssertionError();
		}
	}
	
	private static final Map<TypeCode, Schema> PRIMITIVES = Maps.newHashMap();
	static {
		PRIMITIVES.put(TypeCode.STRING, Schema.create(Type.STRING));
		PRIMITIVES.put(TypeCode.INT, Schema.create(Type.INT));
		PRIMITIVES.put(TypeCode.LONG, Schema.create(Type.LONG));
		PRIMITIVES.put(TypeCode.DOUBLE, Schema.create(Type.DOUBLE));
		PRIMITIVES.put(TypeCode.FLOAT, Schema.create(Type.FLOAT));
		PRIMITIVES.put(TypeCode.BOOLEAN, Schema.create(Type.BOOLEAN));
		PRIMITIVES.put(TypeCode.BINARY, Schema.create(Type.BYTES));
		
		PRIMITIVES.put(TypeCode.DATETIME, LogicalTypes.timestampMillis()
														.addToSchema(Schema.create(Schema.Type.LONG)));
		PRIMITIVES.put(TypeCode.TIME, LogicalTypes.timeMillis()
													.addToSchema(Schema.create(Schema.Type.INT)));
		PRIMITIVES.put(TypeCode.DATE, LogicalTypes.date()
													.addToSchema(Schema.create(Schema.Type.INT)));
		
		for ( DataType geomType: Arrays.asList(DataType.POINT, DataType.MULTI_POINT,
												DataType.LINESTRING, DataType.MULTI_LINESTRING,
												DataType.POLYGON, DataType.MULTI_POLYGON,
												DataType.GEOM_COLLECTION, DataType.GEOMETRY) ) {
			Schema geomSchema = SchemaBuilder.record(geomType.getName())
											.fields()
												.name("wkb").type().bytesType().noDefault()
											.endRecord();
			PRIMITIVES.put(geomType.getTypeCode(), geomSchema);
		}
	}
}
