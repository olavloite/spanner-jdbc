package nl.topicus.jdbc.statement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;

import net.sf.jsqlparser.statement.update.Update;
import nl.topicus.jdbc.AbstractCloudSpannerFetcher;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDriver;
import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;

/**
 * 
 * @author loite
 *
 */
abstract class AbstractCloudSpannerStatement extends AbstractCloudSpannerFetcher implements Statement
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

	private int maxFieldSize = 0;

	AbstractCloudSpannerStatement(CloudSpannerConnection connection, DatabaseClient dbClient)
	{
		this.connection = connection;
		this.dbClient = dbClient;
	}

	protected String sanitizeSQL(String sql)
	{
		// Add a pseudo update to the end if no columns have been specified in
		// an 'on duplicate key update'-statement
		String formatted = sql.trim().toUpperCase();
		formatted = formatted.replaceAll("\n", " ").replaceAll("\t", " ").replaceAll("  ", " ");
		if (formatted.startsWith("INSERT") && formatted.endsWith("ON DUPLICATE KEY UPDATE"))
		{
			sql = sql + " FOO=BAR";
		}
		// Remove @{FORCE_INDEX...} statements
		if (formatted.contains("@{FORCE_INDEX"))
		{
			sql = formatted.replaceAll("\\@\\{FORCE_INDEX=.*\\}", "");
		}
		return sql;
	}

	/**
	 * Transform the given UPDATE-statement into an "INSERT INTO TAB1 (...)
	 * SELECT ... FROM TAB1 WHERE ... ON DUPLICATE KEY UPDATE"
	 * 
	 * @param update
	 *            The UPDATE-statement
	 * @return An SQL-statement equal to the UPDATE-statement but in INSERT form
	 * @throws SQLException
	 */
	protected String createInsertSelectOnDuplicateKeyUpdateStatement(Update update) throws SQLException
	{
		String tableName = update.getTables().get(0).getName();
		TableKeyMetaData table = getConnection().getTable(tableName);
		List<String> keyColumns = table.getKeyColumns();
		List<String> updateColumns = update.getColumns().stream().map(x -> x.getColumnName())
				.collect(Collectors.toList());
		List<String> quotedKeyColumns = keyColumns.stream().map(x -> quoteIdentifier(x)).collect(Collectors.toList());
		List<String> quotedAndQualifiedKeyColumns = keyColumns.stream()
				.map(x -> quoteIdentifier(tableName) + "." + quoteIdentifier(x)).collect(Collectors.toList());

		List<String> quotedUpdateColumns = updateColumns.stream().map(x -> quoteIdentifier(x))
				.collect(Collectors.toList());
		List<String> expressions = update.getExpressions().stream().map(x -> x.toString()).collect(Collectors.toList());
		if (updateColumns.stream().anyMatch(x -> keyColumns.contains(x)))
		{
			String invalidCols = updateColumns.stream().filter(x -> keyColumns.contains(x))
					.collect(Collectors.joining());
			throw new SQLException(
					"UPDATE of a primary key value is not allowed, cannot UPDATE the column(s) " + invalidCols);
		}

		StringBuilder res = new StringBuilder();
		res.append("INSERT INTO ").append(quoteIdentifier(tableName)).append("\n(");
		res.append(String.join(", ", quotedKeyColumns)).append(", ");
		res.append(String.join(", ", quotedUpdateColumns)).append(")");
		res.append("\nSELECT ").append(String.join(", ", quotedAndQualifiedKeyColumns)).append(", ");
		res.append(String.join(", ", expressions));
		res.append("\nFROM ").append(quoteIdentifier(tableName));
		if (update.getWhere() != null)
			res.append("\n").append("WHERE ").append(update.getWhere().toString());
		res.append("\nON DUPLICATE KEY UPDATE");

		return res.toString();
	}

	protected String quoteIdentifier(String identifier)
	{
		return CloudSpannerDriver.quoteIdentifier(identifier);
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

	protected long writeMutations(Mutations mutations) throws SQLException
	{
		if (connection.isReadOnly())
		{
			throw new SQLException("Connection is in read-only mode. Mutations are not allowed");
		}
		if (mutations.isWorker())
		{
			ConversionResult result = mutations.getWorker().call();
			if (result.getException() != null)
			{
				if (result.getException() instanceof SQLException)
					throw (SQLException) result.getException();
				throw new SQLException(result.getException());
			}
		}
		else
		{

			if (connection.getAutoCommit())
			{
				dbClient.readWriteTransaction().run(new TransactionCallable<Void>()
				{

					@Override
					public Void run(TransactionContext transaction) throws Exception
					{
						transaction.buffer(mutations.getMutations());
						return null;
					}
				});
			}
			else
			{
				connection.getTransaction().buffer(mutations.getMutations());
			}
		}
		return mutations.getNumberOfResults();
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
		return maxFieldSize;
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException
	{
		this.maxFieldSize = max;
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
		// silently ignore
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
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		// silently ignore
	}

	@Override
	public void setCursorName(String name) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
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
