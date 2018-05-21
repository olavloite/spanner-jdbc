package nl.topicus.jdbc.statement;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.math.NumberUtils;

import com.google.api.client.util.DateTime;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.ValueBinder;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

import nl.topicus.jdbc.util.CloudSpannerConversionUtil;

class ValueBinderExpressionVisitorAdapter<R> extends AbstractSpannerExpressionVisitorAdapter
{
	private static final Pattern SPLIT_ON_COMMA_PATTERN = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

	private ValueBinder<R> binder;

	ValueBinderExpressionVisitorAdapter(ParameterStore parameterStore, ValueBinder<R> binder, String column)
	{
		super(parameterStore, column);
		this.binder = binder;
	}

	@Override
	protected void setValue(Object value, Integer sqlType)
	{
		R res = setSingleValue(value, sqlType);
		if (res == null && value != null)
			res = setArrayValue(value);

		if (res == null && value != null)
		{
			throw new IllegalArgumentException(
					"Unsupported parameter type: " + value.getClass().getName() + " - " + value.toString());
		}
	}

	private R setSingleValue(Object value, Integer sqlType)
	{
		if (value == null)
		{
			return setNullValue(sqlType);
		}
		else if (Boolean.class.isAssignableFrom(value.getClass()))
		{
			return binder.to((Boolean) value);
		}
		else if (Byte.class.isAssignableFrom(value.getClass()))
		{
			return binder.to(((Byte) value).longValue());
		}
		else if (Short.class.isAssignableFrom(value.getClass()))
		{
			return binder.to(((Short) value).longValue());
		}
		else if (Integer.class.isAssignableFrom(value.getClass()))
		{
			return binder.to(((Integer) value).longValue());
		}
		else if (Long.class.isAssignableFrom(value.getClass()))
		{
			return binder.to(((Long) value).longValue());
		}
		else if (Float.class.isAssignableFrom(value.getClass()))
		{
			return binder.to(((Float) value).doubleValue());
		}
		else if (Double.class.isAssignableFrom(value.getClass()))
		{
			return binder.to(((Double) value).doubleValue());
		}
		else if (BigDecimal.class.isAssignableFrom(value.getClass()))
		{
			return binder.to(((BigDecimal) value).doubleValue());
		}
		else if (Date.class.isAssignableFrom(value.getClass()))
		{
			Date dateValue = (Date) value;
			return binder.to(CloudSpannerConversionUtil.toCloudSpannerDate(dateValue));
		}
		else if (Timestamp.class.isAssignableFrom(value.getClass()))
		{
			Timestamp timeValue = (Timestamp) value;
			return binder.to(CloudSpannerConversionUtil.toCloudSpannerTimestamp(timeValue));
		}
		else if (String.class.isAssignableFrom(value.getClass()))
		{
			String stringVal = (String) value;
			if (stringVal.startsWith("{") && stringVal.endsWith("}"))
			{
				R res = splitAndSetArrayValue(stringVal);
				if (res != null)
				{
					return res;
				}
			}
			return binder.to(stringVal);
		}
		else if (Character.class.isAssignableFrom(value.getClass()))
		{
			return binder.to(((Character) value).toString());
		}
		else if (Character[].class.isAssignableFrom(value.getClass()))
		{
			List<Character> list = Arrays.asList((Character[]) value);
			String s = list.stream().map(Object::toString).reduce("", String::concat);
			return binder.to(s);
		}
		else if (char[].class.isAssignableFrom(value.getClass()))
		{
			return binder.to(String.valueOf((char[]) value));
		}
		else if (byte[].class.isAssignableFrom(value.getClass()))
		{
			return binder.to(ByteArray.copyFrom((byte[]) value));
		}
		else if (ByteArrayInputStream.class.isAssignableFrom(value.getClass()))
		{
			try
			{
				return binder.to(ByteArray.copyFrom((ByteArrayInputStream) value));
			}
			catch (IOException e)
			{
				throw new IllegalArgumentException("Could not copy bytes from input stream: " + e.getMessage(), e);
			}
		}
		else if (Array.class.isAssignableFrom(value.getClass()))
		{
			try
			{
				return setArrayValue(((Array) value).getArray());
			}
			catch (SQLException e)
			{
				throw new IllegalArgumentException(
						"Unsupported parameter type: " + value.getClass().getName() + " - " + value.toString());
			}
		}
		return null;
	}

	private R splitAndSetArrayValue(String arrayString)
	{
		// Split on comma's, do not include '{' and '}'
		String[] array = SPLIT_ON_COMMA_PATTERN.split(arrayString.substring(1, arrayString.length() - 1));
		// Trim the array elements
		for (int i = 0; i < array.length; i++)
			array[i] = array[i] == null ? null : array[i].trim();
		// Try to determine the data type of the array
		if (isStringArray(array))
		{
			return setArrayValue(convertStringArray(array));
		}
		else if (isBooleanArray(array))
		{
			return setArrayValue(convertBooleanArray(array));
		}
		else if (isLongArray(array))
		{
			return setArrayValue(convertLongArray(array));
		}
		else if (isDoubleArray(array))
		{
			return setArrayValue(convertDoubleArray(array));
		}
		else if (isDateArray(array))
		{
			return setArrayValue(convertDateArray(array));
		}
		else if (isTimestampArray(array))
		{
			return setArrayValue(convertTimestampArray(array));
		}
		return null;
	}

	private boolean isStringArray(String[] array)
	{
		for (String val : array)
		{
			if (val != null && !(val.startsWith("\"") && val.endsWith("\"")))
			{
				return false;
			}
		}
		return true;
	}

	private String[] convertStringArray(String[] array)
	{
		int index = 0;
		String[] res = new String[array.length];
		for (String val : array)
		{
			if (val != null)
			{
				res[index] = val.substring(1, val.length() - 1);
			}
			index++;
		}
		return res;
	}

	private boolean isBooleanArray(String[] array)
	{
		for (String val : array)
		{
			if (val != null && !(val.equalsIgnoreCase("true") || val.equalsIgnoreCase("false")))
			{
				return false;
			}
		}
		return true;
	}

	private Boolean[] convertBooleanArray(String[] array)
	{
		int index = 0;
		Boolean[] res = new Boolean[array.length];
		for (String val : array)
		{
			if (val != null)
			{
				res[index] = Boolean.valueOf(val);
			}
			index++;
		}
		return res;
	}

	private boolean isLongArray(String[] array)
	{
		for (String val : array)
		{
			if (!NumberUtils.isDigits(val))
			{
				return false;
			}
		}
		return true;
	}

	private Long[] convertLongArray(String[] array)
	{
		int index = 0;
		Long[] res = new Long[array.length];
		for (String val : array)
		{
			if (val != null)
			{
				res[index] = NumberUtils.createNumber(val).longValue();
			}
			index++;
		}
		return res;
	}

	private boolean isDoubleArray(String[] array)
	{
		for (String val : array)
		{
			if (!NumberUtils.isCreatable(val))
			{
				return false;
			}
		}
		return true;
	}

	private Double[] convertDoubleArray(String[] array)
	{
		int index = 0;
		Double[] res = new Double[array.length];
		for (String val : array)
		{
			if (val != null)
			{
				res[index] = NumberUtils.createNumber(val).doubleValue();
			}
			index++;
		}
		return res;
	}

	private boolean isDateArray(String[] array)
	{
		for (String val : array)
		{
			if (val != null && !(val.startsWith("{d \"") && val.endsWith("\"}")))
			{
				return false;
			}
		}
		return true;
	}

	private Date[] convertDateArray(String[] array)
	{
		int index = 0;
		Date[] res = new Date[array.length];
		for (String val : array)
		{
			if (val != null)
			{
				String date = val.substring(4, val.length() - 2);
				res[index] = new Date(DateTime.parseRfc3339(date).getValue());
			}
			index++;
		}
		return res;
	}

	private boolean isTimestampArray(String[] array)
	{
		for (String val : array)
		{
			if (val != null && !(val.startsWith("{ts \"") && val.endsWith("\"}")))
			{
				return false;
			}
		}
		return true;
	}

	private Timestamp[] convertTimestampArray(String[] array)
	{
		int index = 0;
		Timestamp[] res = new Timestamp[array.length];
		for (String val : array)
		{
			if (val != null)
			{
				StringBuilder date = new StringBuilder(val.substring(5, val.length() - 2).replace(' ', 'T'));
				if (date.charAt(date.length() - 1) != 'Z')
					date.append('Z');
				res[index] = com.google.cloud.Timestamp.parseTimestamp(date.toString()).toSqlTimestamp();
			}
			index++;
		}
		return res;
	}

	private R setArrayValue(Object value)
	{
		if (boolean[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toBoolArray((boolean[]) value);
		}
		else if (Boolean[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toBoolArray(Booleans.toArray(Arrays.asList((Boolean[]) value)));
		}
		else if (short[].class.isAssignableFrom(value.getClass()))
		{
			long[] l = new long[((short[]) value).length];
			Arrays.parallelSetAll(l, i -> ((short[]) value)[i]);
			return binder.toInt64Array(l);
		}
		else if (Short[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toInt64Array(Longs.toArray(Arrays.asList((Short[]) value)));
		}
		else if (int[].class.isAssignableFrom(value.getClass()))
		{
			long[] l = new long[((int[]) value).length];
			Arrays.parallelSetAll(l, i -> ((int[]) value)[i]);
			return binder.toInt64Array(l);
		}
		else if (Integer[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toInt64Array(Longs.toArray(Arrays.asList((Integer[]) value)));
		}
		else if (long[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toInt64Array((long[]) value);
		}
		else if (Long[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toInt64Array(Longs.toArray(Arrays.asList((Long[]) value)));
		}
		else if (float[].class.isAssignableFrom(value.getClass()))
		{
			double[] l = new double[((float[]) value).length];
			Arrays.parallelSetAll(l, i -> ((float[]) value)[i]);
			return binder.toFloat64Array(l);
		}
		else if (Float[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toFloat64Array(Doubles.toArray(Arrays.asList((Float[]) value)));
		}
		else if (double[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toFloat64Array((double[]) value);
		}
		else if (Double[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toFloat64Array(Doubles.toArray(Arrays.asList((Double[]) value)));
		}
		else if (BigDecimal[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toFloat64Array(Doubles.toArray(Arrays.asList((BigDecimal[]) value)));
		}
		else if (Date[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toDateArray(CloudSpannerConversionUtil.toCloudSpannerDates((Date[]) value));
		}
		else if (Timestamp[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toTimestampArray(CloudSpannerConversionUtil.toCloudSpannerTimestamps((Timestamp[]) value));
		}
		else if (String[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toStringArray(Arrays.asList((String[]) value));
		}
		else if (byte[][].class.isAssignableFrom(value.getClass()))
		{
			return binder.toBytesArray(CloudSpannerConversionUtil.toCloudSpannerBytes((byte[][]) value));
		}
		return null;
	}

	private R setNullValue(Integer sqlType)
	{
		if (sqlType == null)
		{
			return binder.to((String) null);
		}
		switch (sqlType)
		{
		case Types.BIGINT:
			return binder.to((Long) null);
		case Types.BINARY:
			return binder.to((ByteArray) null);
		case Types.BLOB:
			return binder.to((ByteArray) null);
		case Types.BOOLEAN:
			return binder.to((Boolean) null);
		case Types.CHAR:
			return binder.to((String) null);
		case Types.CLOB:
			return binder.to((String) null);
		case Types.DATE:
			return binder.to((com.google.cloud.Date) null);
		case Types.DECIMAL:
			return binder.to((Double) null);
		case Types.DOUBLE:
			return binder.to((Double) null);
		case Types.FLOAT:
			return binder.to((Double) null);
		case Types.INTEGER:
			return binder.to((Long) null);
		case Types.LONGNVARCHAR:
			return binder.to((String) null);
		case Types.LONGVARBINARY:
			return binder.to((ByteArray) null);
		case Types.LONGVARCHAR:
			return binder.to((String) null);
		case Types.NCHAR:
			return binder.to((String) null);
		case Types.NCLOB:
			return binder.to((String) null);
		case Types.NUMERIC:
			return binder.to((Double) null);
		case Types.NVARCHAR:
			return binder.to((String) null);
		case Types.REAL:
			return binder.to((Double) null);
		case Types.SMALLINT:
			return binder.to((Long) null);
		case Types.SQLXML:
			return binder.to((String) null);
		case Types.TIME:
			return binder.to((com.google.cloud.Timestamp) null);
		case Types.TIMESTAMP:
			return binder.to((com.google.cloud.Timestamp) null);
		case Types.TINYINT:
			return binder.to((Long) null);
		case Types.VARBINARY:
			return binder.to((ByteArray) null);
		case Types.VARCHAR:
			return binder.to((String) null);
		default:
			throw new IllegalArgumentException("Unsupported sql type: " + sqlType);
		}
	}

}
