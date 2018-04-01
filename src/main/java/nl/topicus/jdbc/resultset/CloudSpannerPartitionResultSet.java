package nl.topicus.jdbc.resultset;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Partition;

import nl.topicus.jdbc.statement.CloudSpannerStatement;

/**
 * A specialized version of a {@link ResultSet} that contains the results of one
 * partition of a partitioned query
 * 
 * @author loite
 *
 */
public class CloudSpannerPartitionResultSet extends CloudSpannerResultSet
{
	private final Partition partition;

	private final BatchReadOnlyTransaction transaction;

	private boolean executed = false;

	public CloudSpannerPartitionResultSet(CloudSpannerStatement statement, BatchReadOnlyTransaction transaction,
			Partition partition, String sql)
	{
		super(statement, sql);
		this.transaction = transaction;
		this.partition = partition;
	}

	@Override
	public boolean next() throws SQLException
	{
		ensureOpenAndExecuted();
		return super.next();
	}

	@Override
	public CloudSpannerResultSetMetaData getMetaData() throws SQLException
	{
		ensureOpenAndExecuted();
		return super.getMetaData();
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException
	{
		ensureOpenAndExecuted();
		return super.findColumn(columnLabel);
	}

	private void ensureOpenAndExecuted() throws SQLException
	{
		ensureOpen();
		if (!executed)
		{
			setResultSet(transaction.execute(partition));
			executed = true;
		}
	}

}
