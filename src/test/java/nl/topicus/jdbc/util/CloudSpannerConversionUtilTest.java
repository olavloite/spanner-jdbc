package nl.topicus.jdbc.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Type;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerConversionUtilTest
{
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@SuppressWarnings("deprecation")
	@Test
	public void testToSqlDate()
	{
		// Get default calendar
		Calendar cal = Calendar.getInstance();
		cal.set(1970, 0, 1, 0, 0, 0);
		cal.clear(Calendar.MILLISECOND);

		assertEquals(new Date(2000 - 1900, 0, 1),
				CloudSpannerConversionUtil.toSqlDate(com.google.cloud.Date.fromYearMonthDay(2000, 1, 1)));
		assertEquals(new Date(cal.getTimeInMillis()),
				CloudSpannerConversionUtil.toSqlDate(com.google.cloud.Date.fromYearMonthDay(1970, 1, 1)));
	}

	@Test
	public void testToSqlDateCalendar()
	{
		Calendar cal1 = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		Calendar cal2 = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal2.set(2000, 0, 1, 0, 0, 0);
		cal2.clear(Calendar.MILLISECOND);
		assertEquals(new Date(cal2.getTimeInMillis()),
				CloudSpannerConversionUtil.toSqlDate(com.google.cloud.Date.fromYearMonthDay(2000, 1, 1), cal1));
		assertEquals(new Date(0l),
				CloudSpannerConversionUtil.toSqlDate(com.google.cloud.Date.fromYearMonthDay(1970, 1, 1), cal1));
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testToCloudSpannerDate()
	{
		assertEquals(com.google.cloud.Date.fromYearMonthDay(2000, 1, 1),
				CloudSpannerConversionUtil.toCloudSpannerDate(new Date(2000 - 1900, 0, 1)));
		assertEquals(com.google.cloud.Date.fromYearMonthDay(1970, 1, 1),
				CloudSpannerConversionUtil.toCloudSpannerDate(new Date(0l)));
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testToCloudSpannerDates()
	{
		assertArrayEquals(
				new com.google.cloud.Date[] { com.google.cloud.Date.fromYearMonthDay(2000, 1, 1),
						com.google.cloud.Date.fromYearMonthDay(1970, 1, 1) },
				CloudSpannerConversionUtil.toCloudSpannerDates(new Date[] { new Date(2000 - 1900, 0, 1), new Date(0l) })
						.toArray());
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testToJavaDates()
	{
		Calendar cal = Calendar.getInstance();
		cal.set(1970, 0, 1, 0, 0, 0);
		cal.clear(Calendar.MILLISECOND);
		assertArrayEquals(new Date[] { new Date(2000 - 1900, 0, 1), new Date(cal.getTimeInMillis()) },
				CloudSpannerConversionUtil.toJavaDates(Arrays.asList(com.google.cloud.Date.fromYearMonthDay(2000, 1, 1),
						com.google.cloud.Date.fromYearMonthDay(1970, 1, 1))).toArray());
	}

	@Test
	public void testToCloudSpannerTimestamp()
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal.set(2000, 0, 1, 13, 15, 10);
		cal.set(Calendar.MILLISECOND, 1);
		Timestamp ts = new Timestamp(cal.getTimeInMillis());
		// Setting nanoseconds clears the previous value for milliseconds in the
		// timestamp
		ts.setNanos(100100);

		com.google.cloud.Timestamp gts1 = CloudSpannerConversionUtil.toCloudSpannerTimestamp(ts);
		com.google.cloud.Timestamp gts2 = com.google.cloud.Timestamp.parseTimestamp("2000-01-01T13:15:10.0001001Z");
		assertEquals(gts2, gts1);

		cal.set(2000, 0, 1, 13, 15, 10);
		cal.clear(Calendar.MILLISECOND);
		ts = new Timestamp(cal.getTimeInMillis());
		// Setting nanoseconds clears the previous value for milliseconds in the
		// timestamp
		ts.setNanos(2000001);
		gts1 = CloudSpannerConversionUtil.toCloudSpannerTimestamp(ts);
		gts2 = com.google.cloud.Timestamp.parseTimestamp("2000-01-01T13:15:10.002000001Z");
		assertEquals(gts2, gts1);
	}

	@Test
	public void testToCloudSpannerTimestamps()
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal.set(2000, 0, 1, 13, 15, 10);
		cal.set(Calendar.MILLISECOND, 1);
		Timestamp ts = new Timestamp(cal.getTimeInMillis());
		// Setting nanoseconds clears the previous value for milliseconds in the
		// timestamp
		ts.setNanos(100100);

		List<com.google.cloud.Timestamp> gts1 = CloudSpannerConversionUtil
				.toCloudSpannerTimestamps(new Timestamp[] { ts });
		List<com.google.cloud.Timestamp> gts2 = Arrays
				.asList(com.google.cloud.Timestamp.parseTimestamp("2000-01-01T13:15:10.0001001Z"));
		assertArrayEquals(gts2.toArray(), gts1.toArray());
	}

	@Test
	public void testToSqlTime()
	{
		com.google.cloud.Timestamp ts = com.google.cloud.Timestamp.parseTimestamp("2000-01-01T13:15:10.0010001Z");
		Time t1 = CloudSpannerConversionUtil.toSqlTime(ts);
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal.set(2000, 0, 1, 13, 15, 10);
		cal.set(Calendar.MILLISECOND, 1);
		Time t2 = new Time(cal.getTimeInMillis());
		assertEquals(t2.getTime(), t1.getTime());
		assertEquals(t2, t1);
	}

	@Test
	public void testToSqlTimeCalendar()
	{
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Europe/Amsterdam"));
		com.google.cloud.Timestamp ts = com.google.cloud.Timestamp.parseTimestamp("2000-01-01T13:15:10.0010001Z");
		Time t1 = CloudSpannerConversionUtil.toSqlTime(ts, calendar);
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal.set(2000, 0, 1, 13, 15, 10);
		cal.set(Calendar.MILLISECOND, 1);
		Time t2 = new Time(cal.getTimeInMillis());
		assertEquals(t2.getTime(), t1.getTime());
		assertEquals(t2, t1);
	}

	@Test
	public void testToJavaTimestamps()
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal.set(1970, 0, 1, 11, 59, 59);
		cal.clear(Calendar.MILLISECOND);
		assertArrayEquals(new Timestamp[] { new Timestamp(cal.getTimeInMillis()) },
				CloudSpannerConversionUtil
						.toJavaTimestamps(
								Arrays.asList(com.google.cloud.Timestamp.parseTimestamp("1970-01-01T11:59:59Z")))
						.toArray());
	}

	@Test
	public void testToCloudSpannerBytes()
	{
		byte[][] input = new byte[][] { "AA".getBytes(), "BB".getBytes() };
		List<ByteArray> output = CloudSpannerConversionUtil.toCloudSpannerBytes(input);
		ByteArray inp1 = ByteArray.copyFrom("AA".getBytes());
		ByteArray inp2 = ByteArray.copyFrom("BB".getBytes());
		assertArrayEquals(new ByteArray[] { inp1, inp2 }, output.toArray());
	}

	@Test
	public void testToJavaByteArrays()
	{
		ByteArray inp1 = ByteArray.copyFrom("AA".getBytes());
		ByteArray inp2 = ByteArray.copyFrom("BB".getBytes());
		List<byte[]> output = CloudSpannerConversionUtil.toJavaByteArrays(Arrays.asList(inp1, inp2));

		List<byte[]> list = Arrays.asList("AA".getBytes(), "BB".getBytes());
		assertArrayEquals(list.toArray(), output.toArray());
	}

	@Test
	public void testConvert() throws SQLException
	{
		assertEquals(Boolean.TRUE, CloudSpannerConversionUtil.convert(Boolean.TRUE, Type.bool(), Boolean.class));
		assertEquals(Boolean.TRUE, CloudSpannerConversionUtil.convert(Long.valueOf(1), Type.int64(), Boolean.class));
		assertEquals(Boolean.TRUE,
				CloudSpannerConversionUtil.convert(Double.valueOf(1d), Type.float64(), Boolean.class));
		assertEquals(Boolean.TRUE, CloudSpannerConversionUtil.convert("True", Type.string(), Boolean.class));

		assertEquals(Boolean.FALSE, CloudSpannerConversionUtil.convert(Boolean.FALSE, Type.bool(), Boolean.class));
		assertEquals(Boolean.FALSE, CloudSpannerConversionUtil.convert(Long.valueOf(0), Type.int64(), Boolean.class));
		assertEquals(Boolean.FALSE,
				CloudSpannerConversionUtil.convert(Double.valueOf(0d), Type.float64(), Boolean.class));
		assertEquals(Boolean.FALSE, CloudSpannerConversionUtil.convert("False", Type.string(), Boolean.class));

		assertEquals(BigDecimal.ONE, CloudSpannerConversionUtil.convert(Boolean.TRUE, Type.bool(), BigDecimal.class));
		assertEquals(BigDecimal.TEN,
				CloudSpannerConversionUtil.convert(Long.valueOf(10), Type.int64(), BigDecimal.class));
		assertEquals(BigDecimal.valueOf(10.1d),
				CloudSpannerConversionUtil.convert(Double.valueOf(10.1d), Type.float64(), BigDecimal.class));
		assertEquals(new BigDecimal("10.2"),
				CloudSpannerConversionUtil.convert("10.2", Type.string(), BigDecimal.class));

		assertEquals(Long.valueOf(1), CloudSpannerConversionUtil.convert(Boolean.TRUE, Type.bool(), Long.class));
		assertEquals(Long.valueOf(10), CloudSpannerConversionUtil.convert(Long.valueOf(10), Type.int64(), Long.class));
		assertEquals(Long.valueOf(10),
				CloudSpannerConversionUtil.convert(Double.valueOf(10.1d), Type.float64(), Long.class));
		assertEquals(new Long("10"), CloudSpannerConversionUtil.convert("10", Type.string(), Long.class));

		assertEquals(Integer.valueOf(1), CloudSpannerConversionUtil.convert(Boolean.TRUE, Type.bool(), Integer.class));
		assertEquals(Integer.valueOf(10),
				CloudSpannerConversionUtil.convert(Long.valueOf(10), Type.int64(), Integer.class));
		assertEquals(Integer.valueOf(10),
				CloudSpannerConversionUtil.convert(Double.valueOf(10.1d), Type.float64(), Integer.class));
		assertEquals(new Integer("10"), CloudSpannerConversionUtil.convert("10", Type.string(), Integer.class));

		assertEquals(BigInteger.valueOf(1),
				CloudSpannerConversionUtil.convert(Boolean.TRUE, Type.bool(), BigInteger.class));
		assertEquals(BigInteger.valueOf(10),
				CloudSpannerConversionUtil.convert(Long.valueOf(10), Type.int64(), BigInteger.class));
		assertEquals(BigInteger.valueOf(10),
				CloudSpannerConversionUtil.convert(Double.valueOf(10.1d), Type.float64(), BigInteger.class));
		assertEquals(new BigInteger("10"), CloudSpannerConversionUtil.convert("10", Type.string(), BigInteger.class));

		assertEquals(Float.valueOf(1), CloudSpannerConversionUtil.convert(Boolean.TRUE, Type.bool(), Float.class));
		assertEquals(Float.valueOf(10),
				CloudSpannerConversionUtil.convert(Long.valueOf(10), Type.int64(), Float.class));
		assertEquals(Float.valueOf(Double.valueOf(10.1d).floatValue()),
				CloudSpannerConversionUtil.convert(Double.valueOf(10.1d), Type.float64(), Float.class));
		assertEquals(new Float("10.2"), CloudSpannerConversionUtil.convert("10.2", Type.string(), Float.class));

		assertEquals(Double.valueOf(1), CloudSpannerConversionUtil.convert(Boolean.TRUE, Type.bool(), Double.class));
		assertEquals(Double.valueOf(10),
				CloudSpannerConversionUtil.convert(Long.valueOf(10), Type.int64(), Double.class));
		assertEquals(Double.valueOf(10.1d),
				CloudSpannerConversionUtil.convert(Double.valueOf(10.1d), Type.float64(), Double.class));
		assertEquals(new Double("10.2"), CloudSpannerConversionUtil.convert("10.2", Type.string(), Double.class));

		assertEquals("true", CloudSpannerConversionUtil.convert(Boolean.TRUE, Type.bool(), String.class));
		assertEquals("10", CloudSpannerConversionUtil.convert(Long.valueOf(10), Type.int64(), String.class));
		assertEquals("10.1", CloudSpannerConversionUtil.convert(Double.valueOf(10.1d), Type.float64(), String.class));
		assertEquals("10.2", CloudSpannerConversionUtil.convert("10.2", Type.string(), String.class));
	}

	@Test
	public void testConvertInvalidType() throws SQLException
	{
		testInvalidType(Type.date(), Boolean.class);
		testInvalidType(Type.timestamp(), Boolean.class);
		testInvalidType(Type.int64(), Timestamp.class);
		testInvalidType(Type.int64(), Date.class);
		testInvalidType(Type.float64(), Timestamp.class);
		testInvalidType(Type.float64(), Date.class);
		testInvalidType(Type.timestamp(), Date.class);
	}

	private void testInvalidType(Type fromType, Class<?> toType) throws CloudSpannerSQLException
	{
		try
		{
			CloudSpannerConversionUtil.convert(new Object(), fromType, toType);
		}
		catch (CloudSpannerSQLException e)
		{
			if (e.getMessage().equals("Cannot convert " + fromType.getCode().name() + " to " + toType.getName()))
				return;
			throw e;
		}
		throw new AssertionError("Expected exception not thrown");
	}

	@Test
	public void testConvertInvalidValue() throws SQLException
	{
		testInvalidValue("test", Type.string(), Integer.class);
		testInvalidValue("2010-10", Type.string(), Integer.class);
		testInvalidValue("2010-10", Type.string(), BigDecimal.class);
	}

	private void testInvalidValue(Object value, Type fromType, Class<?> toType) throws CloudSpannerSQLException
	{
		try
		{
			CloudSpannerConversionUtil.convert(value, fromType, toType);
		}
		catch (CloudSpannerSQLException e)
		{
			if (e.getMessage().equals("Cannot convert " + value.toString() + " to " + toType.getName()))
				return;
			throw e;
		}
		throw new AssertionError("Expected exception not thrown");
	}

}
