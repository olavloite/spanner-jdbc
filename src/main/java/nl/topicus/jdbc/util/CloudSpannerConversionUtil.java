package nl.topicus.jdbc.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.cloud.ByteArray;

public class CloudSpannerConversionUtil
{
	private CloudSpannerConversionUtil()
	{
	}

	public static Date toSqlDate(com.google.cloud.Date date)
	{
		@SuppressWarnings("deprecation")
		Date res = new Date(date.getYear() - 1900, date.getMonth() - 1, date.getDayOfMonth());
		return res;
	}

	public static com.google.cloud.Date toCloudSpannerDate(Date date)
	{
		@SuppressWarnings("deprecation")
		com.google.cloud.Date res = com.google.cloud.Date.fromYearMonthDay(date.getYear() + 1900, date.getMonth() + 1,
				date.getDate());
		return res;
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

	public static Timestamp toSqlTimestamp(com.google.cloud.Timestamp ts)
	{
		Timestamp res = ts.toSqlTimestamp();
		return res;
	}

	public static com.google.cloud.Timestamp toCloudSpannerTimestamp(Timestamp ts)
	{
		long milliseconds = ts.getTime();
		long seconds = milliseconds / 1000l;
		int rest = (int) (milliseconds % 1000l);
		int nanos = (int) TimeUnit.MILLISECONDS.toNanos(rest);
		com.google.cloud.Timestamp res = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(seconds, nanos);
		return res;
	}

	public static List<com.google.cloud.Timestamp> toCloudSpannerTimestamps(Timestamp[] timestamps)
	{
		List<com.google.cloud.Timestamp> res = new ArrayList<>(timestamps.length);
		for (int index = 0; index < timestamps.length; index++)
			res.add(toCloudSpannerTimestamp(timestamps[index]));
		return res;
	}

	public static List<Timestamp> toJavaTimestamps(List<com.google.cloud.Timestamp> timestamps)
	{
		List<Timestamp> res = new ArrayList<>(timestamps.size());
		for (com.google.cloud.Timestamp timestamp : timestamps)
			res.add(CloudSpannerConversionUtil.toSqlTimestamp(timestamp));
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

}
