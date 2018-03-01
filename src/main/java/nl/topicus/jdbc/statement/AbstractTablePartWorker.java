package nl.topicus.jdbc.statement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

import com.google.cloud.spanner.SpannerException;
import com.google.rpc.Code;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Select;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDriver;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;

public abstract class AbstractTablePartWorker implements Callable<ConversionResult>
{
	private enum Mode
	{
		UNKNOWN, NORMAL, EXTENDED;
	}

	enum DMLOperation
	{
		INSERT, ONDUPLICATEKEYUPDATE, UPDATE, DELETE;
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

	private Mode mode = Mode.UNKNOWN;

	protected final DMLOperation operation;

	private final ParameterStore parameters;

	private long estimatedRecordCount = -1;

	private long recordCount = 0;

	AbstractTablePartWorker(CloudSpannerConnection connection, Select select, ParameterStore parameters,
			boolean allowExtendedMode, DMLOperation operation)
	{
		this.connection = connection;
		this.select = select;
		this.parameters = parameters;
		this.allowExtendedMode = allowExtendedMode;
		this.operation = operation;
	}

	@Override
	public ConversionResult call()
	{
		Exception exception = null;
		long startTime = System.currentTimeMillis();
		try
		{
			genericRun();
		}
		catch (Exception e)
		{
			exception = e;
		}
		long endTime = System.currentTimeMillis();
		return new ConversionResult(recordCount, 0, startTime, endTime, exception);
	}

	protected void genericRun() throws SQLException
	{
		String unquotedTableName = CloudSpannerDriver.unquoteIdentifier(getTable().getName());
		List<String> columnNamesList = getColumnNames();
		long batchSize = ConverterUtils.calculateActualBatchSize(columnNamesList.size(), connection, null, null,
				unquotedTableName);
		boolean isExtendedMode = isExtendedMode(batchSize);

		boolean wasAutocommit = connection.getAutoCommit();
		if (!isExtendedMode && wasAutocommit)
		{
			connection.setAutoCommit(false);
		}
		try (Connection destination = isExtendedMode ? connection.createCopyConnection() : null)
		{
			if (destination != null)
			{
				destination.setAutoCommit(false);
			}
			String sql = createSQL();
			try (PreparedStatement statement = destination == null ? connection.prepareStatement(sql)
					: destination.prepareStatement(sql))
			{
				if (operation == DMLOperation.UPDATE)
				{
					// Set force update
					((CloudSpannerPreparedStatement) statement).setForceUpdate(true);
				}
				CloudSpannerPreparedStatement selectStatement = connection.prepareStatement(select.toString());
				for (int i = 1; i <= parameters.getHighestIndex(); i++)
				{
					selectStatement.setObject(i, parameters.getParameter(i));
				}
				try (ResultSet rs = selectStatement.executeQuery())
				{
					ResultSetMetaData metadata = rs.getMetaData();
					while (rs.next())
					{
						for (int index = 1; index <= metadata.getColumnCount(); index++)
						{
							Object object = rs.getObject(index);
							statement.setObject(index, object);
						}
						statement.executeUpdate();
						recordCount++;
						if (destination != null && recordCount % batchSize == 0)
							destination.commit();
					}
				}
			}
			if (destination != null)
			{
				destination.commit();
			}
			if (wasAutocommit && !isExtendedMode)
			{
				connection.commit();
				connection.setAutoCommit(true);
			}
		}
		catch (SpannerException e)
		{
			throw new CloudSpannerSQLException(e);
		}
		catch (CloudSpannerSQLException e)
		{
			throw e;
		}
		catch (Exception e)
		{
			throw new CloudSpannerSQLException(e.getMessage(), Code.UNKNOWN, e);
		}
		finally
		{
			if (wasAutocommit && !isExtendedMode)
			{
				connection.rollback();
				connection.setAutoCommit(true);
			}
		}

	}

	protected abstract List<String> getColumnNames() throws SQLException;

	protected abstract Table getTable();

	protected abstract String createSQL() throws SQLException;

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
		if (mode == Mode.UNKNOWN)
		{
			if (!allowExtendedMode)
			{
				mode = Mode.NORMAL;
			}
			else
			{
				long count = getEstimatedRecordCount(select);
				if (count >= batchSize)
				{
					mode = Mode.EXTENDED;
				}
				else
				{
					mode = Mode.NORMAL;
				}
			}
		}
		return mode == Mode.EXTENDED;
	}

	public long getRecordCount()
	{
		return recordCount;
	}

}
