package nl.topicus.jdbc.extended;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import net.sf.jsqlparser.statement.select.Select;
import nl.topicus.jdbc.CloudSpannerConnection;

public abstract class AbstractTablePartWorker implements Callable<ConversionResult>
{
	private static enum Mode
	{
		Unknown, Normal, Extended;
	}

	protected final CloudSpannerConnection connection;

	protected final Select select;

	/**
	 * This flag indicates whether the worker should go into 'extended' mode if
	 * the number of records/mutations is expected to exceed the limitations of
	 * Cloud Spanner. Extended mode means that a separate connection will be
	 * opened and that the inserts will be performed on that connection. These
	 * inserts will also be committed automatically, and there is no guarantee
	 * that the entire insert operation will succeed (the insert is not
	 * performed atomically)
	 */
	private boolean allowExtendedMode;

	private Mode mode = Mode.Unknown;

	private long estimatedRecordCount = -1;

	AbstractTablePartWorker(CloudSpannerConnection connection, Select select, boolean allowExtendedMode)
	{
		this.connection = connection;
		this.select = select;
		this.allowExtendedMode = allowExtendedMode;
	}

	@Override
	public ConversionResult call()
	{
		Exception exception = null;
		long startTime = System.currentTimeMillis();
		try
		{
			run();
		}
		catch (Exception e)
		{
			exception = e;
		}
		long endTime = System.currentTimeMillis();
		return new ConversionResult(getRecordCount(), 0, startTime, endTime, exception);
	}

	protected long getEstimatedRecordCount(Select select) throws SQLException
	{
		if (estimatedRecordCount == -1)
		{
			String sql = "SELECT COUNT(*) AS C FROM (" + select.toString() + ") Q";
			try (ResultSet count = connection.prepareStatement(sql).executeQuery())
			{
				if (count.next())
					estimatedRecordCount = count.getLong(1);
			}
		}
		return estimatedRecordCount;
	}

	protected boolean isExtendedMode(long batchSize) throws SQLException
	{
		if (mode == Mode.Unknown)
		{
			if (!allowExtendedMode)
			{
				mode = Mode.Normal;
			}
			else
			{
				long count = getEstimatedRecordCount(select);
				if (count >= batchSize)
				{
					mode = Mode.Extended;
				}
				else
				{
					mode = Mode.Normal;
				}
			}
		}
		return mode == Mode.Extended;
	}

	protected abstract void run() throws Exception;

	public abstract long getRecordCount();

}
