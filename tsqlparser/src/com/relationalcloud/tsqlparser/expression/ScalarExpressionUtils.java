package com.relationalcloud.tsqlparser.expression;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class ScalarExpressionUtils {
	public static ScalarExpression ConstructFrom(Object o) {
		if (o == null)
			return new NullValue();
		else if (o instanceof Integer)
			return new LongValue(((Integer) o).longValue());
		else if (o instanceof Long)
			return new LongValue((Long) o);
		else if (o instanceof Float)
			return new DoubleValue(((Float) o).floatValue());
		else if (o instanceof Double)
			return new DoubleValue((Double) o);
		else if (o instanceof String)
			return new StringValue("'" + o + "'");
		else if (o instanceof Date)
			return new DateValue((Date) o);
		else if (o instanceof Timestamp)
			return new TimestampValue((Timestamp) o);
		else if (o instanceof Time)
			return new TimeValue((Time) o);
		throw new RuntimeException("Cannot generate scalar from type: " + o.getClass());
	}
}
