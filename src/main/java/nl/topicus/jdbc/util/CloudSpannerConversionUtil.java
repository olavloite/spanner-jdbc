package nl.topicus.jdbc.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class CloudSpannerConversionUtil
{
	public static Date toSqlDate(com.google.cloud.spanner.Date date)
	{
		@SuppressWarnings("deprecation")
		Date res = new Date(date.getYear() - 1900, date.getMonth() - 1, date.getDayOfMonth());
		return res;
	}

	public static com.google.cloud.spanner.Date toCloudSpannerDate(Date date)
	{
		@SuppressWarnings("deprecation")
		com.google.cloud.spanner.Date res = com.google.cloud.spanner.Date.fromYearMonthDay(date.getYear() + 1900,
				date.getMonth() + 1, date.getDate());
		return res;
	}

	public static Timestamp toSqlTimestamp(com.google.cloud.spanner.Timestamp ts)
	{
		Timestamp res = ts.toSqlTimestamp();
		return res;
	}

	public static com.google.cloud.spanner.Timestamp toCloudSpannerTimestamp(Timestamp ts)
	{
		long milliseconds = ts.getTime();
		long seconds = milliseconds / 1000l;
		int rest = (int) (milliseconds % 1000l);
		int nanos = (int) TimeUnit.MILLISECONDS.toNanos(rest);
		com.google.cloud.spanner.Timestamp res = com.google.cloud.spanner.Timestamp.ofTimeSecondsAndNanos(seconds,
				nanos);
		return res;
	}

}
