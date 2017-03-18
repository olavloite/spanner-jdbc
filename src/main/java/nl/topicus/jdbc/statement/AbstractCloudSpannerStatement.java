package nl.topicus.jdbc.statement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

import nl.topicus.jdbc.CloudSpannerConnection;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;

/**
 * 
 * @author loite
 *
 */
abstract class AbstractCloudSpannerStatement implements Statement
{
	private DatabaseClient dbClient;

	/**
	 * Flag to indicate that this statement should use a SingleUseReadContext
	 * regardless whether a transaction is running or not. This is for example
	 * needed for meta data operations (select statements on
	 * INFORMATION_SCHEMA).
	 */
	private boolean forceSingleUseReadContext;

	private boolean closed;

	private int queryTimeout;

	private boolean poolable;

	private boolean closeOnCompletion;

	private CloudSpannerConnection connection;

	private int maxRows;

	AbstractCloudSpannerStatement(CloudSpannerConnection connection, DatabaseClient dbClient)
	{
		this.connection = connection;
		this.dbClient = dbClient;
	}

	protected DatabaseClient getDbClient()
	{
		return dbClient;
	}

	public boolean isForceSingleUseReadContext()
	{
		return forceSingleUseReadContext;
	}

	public void setForceSingleUseReadContext(boolean forceSingleUseReadContext)
	{
		this.forceSingleUseReadContext = forceSingleUseReadContext;
	}

	protected ReadContext getReadContext() throws SQLException
	{
		if (connection.getAutoCommit() || forceSingleUseReadContext)
		{
			return dbClient.singleUse();
		}
		return connection.getTransaction();
	}

	protected void writeMutation(Mutation mutation) throws SQLException
	{
		if (connection.getAutoCommit())
		{
			dbClient.readWriteTransaction().run(new TransactionCallable<Void>()
			{

				@Override
				public Void run(TransactionContext transaction) throws Exception
				{
					transaction.buffer(mutation);
					return null;
				}
			});
		}
		else
		{
			connection.getTransaction().buffer(mutation);
		}
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return closed;
	}

	@Override
	public void close() throws SQLException
	{
		closed = true;
	}

	protected void checkClosed() throws SQLException
	{
		if (isClosed())
			throw new SQLException("Statement is closed");
	}

	@Override
	public int getMaxFieldSize() throws SQLException
	{
		return 0;
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxRows() throws SQLException
	{
		return maxRows;
	}

	@Override
	public void setMaxRows(int max) throws SQLException
	{
		this.maxRows = max;
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getQueryTimeout() throws SQLException
	{
		return queryTimeout;
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException
	{
		queryTimeout = seconds;
	}

	@Override
	public void cancel() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCursorName(String name) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchDirection() throws SQLException
	{
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchSize() throws SQLException
	{
		return 0;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException
	{
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public int getResultSetType() throws SQLException
	{
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public void addBatch(String sql) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearBatch() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int[] executeBatch() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CloudSpannerConnection getConnection() throws SQLException
	{
		return connection;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetHoldability() throws SQLException
	{
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException
	{
		this.poolable = poolable;
	}

	@Override
	public boolean isPoolable() throws SQLException
	{
		return poolable;
	}

	@Override
	public void closeOnCompletion() throws SQLException
	{
		closeOnCompletion = true;
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException
	{
		return closeOnCompletion;
	}

}
