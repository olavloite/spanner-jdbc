package nl.topicus.jdbc.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.common.base.Preconditions;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;

public class CloudSpannerConversionUtil
{
	private CloudSpannerConversionUtil()
	{
	}

	public static Date toSqlDate(com.google.cloud.Date date)
	{
		return toSqlDate(date, Calendar.getInstance());
	}

	public static Date toSqlDate(com.google.cloud.Date date, Calendar cal)
	{
		cal.set(date.getYear(), date.getMonth() - 1, date.getDayOfMonth(), 0, 0, 0);
		cal.clear(Calendar.MILLISECOND);
		return new Date(cal.getTimeInMillis());
	}

	public static com.google.cloud.Date toCloudSpannerDate(Date date)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		return com.google.cloud.Date.fromYearMonthDay(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
				cal.get(Calendar.DAY_OF_MONTH));
	}

	public static List<com.google.cloud.Date> toCloudSpannerDates(Date[] dates)
	{
		List<com.google.cloud.Date> res = new ArrayList<>(dates.length);
		for (int index = 0; index < dates.length; index++)
			res.add(toCloudSpannerDate(dates[index]));
		return res;
	}

	public static List<Date> toJavaDates(List<com.google.cloud.Date> dates)
	{
		List<Date> res = new ArrayList<>(dates.size());
		for (com.google.cloud.Date date : dates)
			res.add(CloudSpannerConversionUtil.toSqlDate(date));
		return res;
	}

	public static com.google.cloud.Timestamp toCloudSpannerTimestamp(Timestamp ts)
	{
		long milliseconds = ts.getTime();
		long seconds = milliseconds / 1000l;
		int nanos = ts.getNanos();
		return com.google.cloud.Timestamp.ofTimeSecondsAndNanos(seconds, nanos);
	}

	public static List<com.google.cloud.Timestamp> toCloudSpannerTimestamps(Timestamp[] timestamps)
	{
		List<com.google.cloud.Timestamp> res = new ArrayList<>(timestamps.length);
		for (int index = 0; index < timestamps.length; index++)
			res.add(toCloudSpannerTimestamp(timestamps[index]));
		return res;
	}

	public static Time toSqlTime(com.google.cloud.Timestamp ts)
	{
		return toSqlTime(ts, Calendar.getInstance());
	}

	public static Time toSqlTime(com.google.cloud.Timestamp ts, Calendar cal)
	{
		cal.set(1970, 0, 1, 0, 0, 0);
		cal.clear(Calendar.MILLISECOND);
		cal.setTimeInMillis(
				ts.getSeconds() * 1000 + TimeUnit.MILLISECONDS.convert(ts.getNanos(), TimeUnit.NANOSECONDS));
		return new Time(cal.getTimeInMillis());
	}

	public static List<Timestamp> toJavaTimestamps(List<com.google.cloud.Timestamp> timestamps)
	{
		List<Timestamp> res = new ArrayList<>(timestamps.size());
		for (com.google.cloud.Timestamp timestamp : timestamps)
			res.add(timestamp.toSqlTimestamp());
		return res;
	}

	public static List<ByteArray> toCloudSpannerBytes(byte[][] bytes)
	{
		List<ByteArray> res = new ArrayList<>(bytes.length);
		for (int index = 0; index < bytes.length; index++)
			res.add(ByteArray.copyFrom(bytes[index]));
		return res;
	}

	public static List<byte[]> toJavaByteArrays(List<ByteArray> bytes)
	{
		List<byte[]> res = new ArrayList<>(bytes.size());
		for (ByteArray ba : bytes)
			res.add(ba.toByteArray());
		return res;
	}

	/**
	 * Converts the given value from the Google {@link Type} to the Java
	 * {@link Class} type.
	 * 
	 * @param value
	 *            The value to convert
	 * @param type
	 *            The type in the database
	 * @param targetType
	 *            The java class target type to convert to
	 * @return The converted value
	 * @throws CloudSpannerSQLException
	 */
	public static Object convert(Object value, Type type, Class<?> targetType) throws CloudSpannerSQLException
	{
		Preconditions.checkNotNull(type, "type may not be null");
		Preconditions.checkNotNull(targetType, "targetType may not be null");
		if (value == null)
			return null;
		if (targetType.equals(String.class))
			return value.toString();

		try
		{
			if (targetType.equals(Boolean.class) && type.getCode() == Code.BOOL)
				return value;
			if (targetType.equals(Boolean.class) && type.getCode() == Code.INT64)
				return Boolean.valueOf((Long) value != 0);
			if (targetType.equals(Boolean.class) && type.getCode() == Code.FLOAT64)
				return Boolean.valueOf((Double) value != 0d);
			if (targetType.equals(Boolean.class) && type.getCode() == Code.STRING)
				return Boolean.valueOf((String) value);

			if (targetType.equals(BigDecimal.class) && type.getCode() == Code.BOOL)
				return (Boolean) value ? BigDecimal.ONE : BigDecimal.ZERO;
			if (targetType.equals(BigDecimal.class) && type.getCode() == Code.INT64)
				return BigDecimal.valueOf((Long) value);
			if (targetType.equals(BigDecimal.class) && type.getCode() == Code.FLOAT64)
				return BigDecimal.valueOf((Double) value);
			if (targetType.equals(BigDecimal.class) && type.getCode() == Code.STRING)
				return new BigDecimal((String) value);

			if (targetType.equals(Long.class) && type.getCode() == Code.BOOL)
				return (Boolean) value ? 1L : 0L;
			if (targetType.equals(Long.class) && type.getCode() == Code.INT64)
				return value;
			if (targetType.equals(Long.class) && type.getCode() == Code.FLOAT64)
				return ((Double) value).longValue();
			if (targetType.equals(Long.class) && type.getCode() == Code.STRING)
				return Long.valueOf((String) value);

			if (targetType.equals(Integer.class) && type.getCode() == Code.BOOL)
				return (Boolean) value ? 1 : 0;
			if (targetType.equals(Integer.class) && type.getCode() == Code.INT64)
				return ((Long) value).intValue();
			if (targetType.equals(Integer.class) && type.getCode() == Code.FLOAT64)
				return ((Double) value).intValue();
			if (targetType.equals(Integer.class) && type.getCode() == Code.STRING)
				return Integer.valueOf((String) value);

			if (targetType.equals(BigInteger.class) && type.getCode() == Code.BOOL)
				return (Boolean) value ? BigInteger.ONE : BigInteger.ZERO;
			if (targetType.equals(BigInteger.class) && type.getCode() == Code.INT64)
				return BigInteger.valueOf((Long) value);
			if (targetType.equals(BigInteger.class) && type.getCode() == Code.FLOAT64)
				return BigInteger.valueOf(((Double) value).longValue());
			if (targetType.equals(BigInteger.class) && type.getCode() == Code.STRING)
				return new BigInteger((String) value);

			if (targetType.equals(Float.class) && type.getCode() == Code.BOOL)
				return (Boolean) value ? Float.valueOf(1f) : Float.valueOf(0f);
			if (targetType.equals(Float.class) && type.getCode() == Code.INT64)
				return ((Long) value).floatValue();
			if (targetType.equals(Float.class) && type.getCode() == Code.FLOAT64)
				return ((Double) value).floatValue();
			if (targetType.equals(Float.class) && type.getCode() == Code.STRING)
				return Float.valueOf((String) value);

			if (targetType.equals(Double.class) && type.getCode() == Code.BOOL)
				return (Boolean) value ? Double.valueOf(1d) : Double.valueOf(0d);
			if (targetType.equals(Double.class) && type.getCode() == Code.INT64)
				return ((Long) value).doubleValue();
			if (targetType.equals(Double.class) && type.getCode() == Code.FLOAT64)
				return value;
			if (targetType.equals(Double.class) && type.getCode() == Code.STRING)
				return Double.valueOf((String) value);
		}
		catch (Exception e)
		{
			throw new CloudSpannerSQLException("Cannot convert " + value + " to " + targetType.getName(),
					com.google.rpc.Code.INVALID_ARGUMENT, e);
		}

		throw new CloudSpannerSQLException("Cannot convert " + type.getCode().name() + " to " + targetType.getName(),
				com.google.rpc.Code.INVALID_ARGUMENT);
	}

}
