package nl.topicus.jdbc.extended;

import java.util.List;
import java.util.concurrent.Future;

public class ConversionResult
{
	private final long recordCount;

	private final long byteCount;

	private final long startTime;

	private final long endTime;

	private final Exception exception;

	public static ConversionResult collect(List<Future<ConversionResult>> results, long startTime, long endTime,
			Exception exception)
	{
		long recordCount = 0;
		long byteCount = 0;
		try
		{
			for (Future<ConversionResult> result : results)
			{
				recordCount += result.get().recordCount;
				byteCount += result.get().byteCount;
			}
		}
		catch (Exception e)
		{
			// ignore
		}
		return new ConversionResult(recordCount, byteCount, startTime, endTime, exception);
	}

	ConversionResult(long recordCount, long byteCount, long startTime, long endTime)
	{
		this(recordCount, byteCount, startTime, endTime, null);
	}

	ConversionResult(long recordCount, long byteCount, long startTime, long endTime, Exception exception)
	{
		this.recordCount = recordCount;
		this.byteCount = byteCount;
		this.startTime = startTime;
		this.endTime = endTime;
		this.exception = exception;
	}

	public long getRecordCount()
	{
		return recordCount;
	}

	public long getByteCount()
	{
		return byteCount;
	}

	public long getStartTime()
	{
		return startTime;
	}

	public long getEndTime()
	{
		return endTime;
	}

	public Exception getException()
	{
		return exception;
	}

	@Override
	public String toString()
	{
		StringBuilder res = new StringBuilder();
		res.append("Records: ").append(recordCount).append(", ");
		res.append("Bytes: ").append(byteCount).append(", ");
		res.append("Time: ").append((endTime - startTime)).append("ms");
		if (exception != null)
		{
			res.append(", Exception: ").append(exception.getMessage());
		}
		return res.toString();
	}

}
