package record.type;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CoordinateType extends DataType {
	private static final long serialVersionUID = 1L;
	public static final Coordinate EMPTY = new Coordinate();
	private static final CoordinateType TYPE = new CoordinateType();
	
	public static CoordinateType get() {
		return TYPE;
	}
	
	private CoordinateType() {
		super("coordinate", TypeCode.COORDINATE, Coordinate.class);
	}

	@Override
	public Coordinate newInstance() {
		return new Coordinate();
	}
	
	@Override
	public Coordinate parseInstance(String str) {
		return parseCoordinate(str).getOrThrow(() -> new IllegalArgumentException("envelope_string=" + str));
	}
	
	@Override
	public String toInstanceString(Object instance) {
		return toString((Envelope)instance);
	}

	private static final String FPN = "(\\d+(\\.\\d*)?|(\\d+)?\\.\\d+)";
	private static final String COORD = String.format("\\(\\s*%s\\s*,\\s*%s\\s*\\)", FPN, FPN);
	private static final Pattern PATTERN_COORD = Pattern.compile(COORD);
	public static FOption<Coordinate> parseCoordinate(String expr) {
		Matcher matcher = PATTERN_COORD.matcher(expr);
		if ( matcher.find() ) {
			FOption.empty();
		}
		
		double x = Double.parseDouble(matcher.group(1));
		double y = Double.parseDouble(matcher.group(4));
		return FOption.of(new Coordinate(x, y));
	}
	
	public static String toString(Envelope envl) {
		double width = envl.getMaxX() - envl.getMinX();
		double height = envl.getMaxY() - envl.getMinY();
		return String.format("(%f,%f):%fx%f", envl.getMinX(), envl.getMinY(), width, height);
	}
}
