package nl.topicus.jdbc.statement;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.ValueBinder;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

import nl.topicus.jdbc.util.CloudSpannerConversionUtil;

class ValueBinderExpressionVisitorAdapter<R> extends AbstractSpannerExpressionVisitorAdapter
{
	private ValueBinder<R> binder;

	ValueBinderExpressionVisitorAdapter(ParameterStore parameterStore, ValueBinder<R> binder, String column)
	{
		super(parameterStore, column);
		this.binder = binder;
	}

	@Override
	protected void setValue(Object value)
	{
		R res = setSingleValue(value);
		if (res == null && value != null)
			res = setArrayValue(value);

		if (res == null && value != null)
		{
			throw new IllegalArgumentException(
					"Unsupported parameter type: " + value.getClass().getName() + " - " + value.toString());
		}
	}

	private R setSingleValue(Object value)
	{
		if (value == null)
		{
			// Set to null, type does not matter
			return binder.to((Boolean) null);
		}
		else if (Boolean.class.isAssignableFrom(value.getClass()))
		{
			return binder.to((Boolean) value);
		}
		else if (Byte.class.isAssignableFrom(value.getClass()))
		{
			return binder.to(((Byte) value).longValue());
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
			return binder.to((String) value);
		}
		else if (byte[].class.isAssignableFrom(value.getClass()))
		{
			return binder.to(ByteArray.copyFrom((byte[]) value));
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

	private R setArrayValue(Object value)
	{
		if (Boolean[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toBoolArray(Booleans.toArray(Arrays.asList((Boolean[]) value)));
		}
		else if (Byte[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toInt64Array(Longs.toArray(Arrays.asList((Byte[]) value)));
		}
		else if (Integer[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toInt64Array(Longs.toArray(Arrays.asList((Integer[]) value)));
		}
		else if (Long[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toInt64Array(Longs.toArray(Arrays.asList((Long[]) value)));
		}
		else if (Float[].class.isAssignableFrom(value.getClass()))
		{
			return binder.toFloat64Array(Doubles.toArray(Arrays.asList((Float[]) value)));
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

}
