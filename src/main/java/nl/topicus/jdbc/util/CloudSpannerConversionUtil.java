package nl.topicus.jdbc.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class CloudSpannerConversionUtil
{
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

}
