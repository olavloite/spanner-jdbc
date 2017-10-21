package nl.topicus.jdbc.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.cloud.ByteArray;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerConversionUtilTest
{
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

}
