package record;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SerializableRecordAdaptors {
	private SerializableRecordAdaptors() {
		throw new AssertionError("should not be called: class=" + getClass());
	}
	
	public static RecordSetReader reader(RecordSchema schema, InputStream is) {
		return new SerializableRecordSetReader(schema, () -> new ObjectInputStream(is));
	}
	
	public static RecordSetWriter writer(RecordSchema schema, OutputStream os) throws IOException {
		return new SerializableRecordSetWriter(new ObjectOutputStream(os));
	}
}
