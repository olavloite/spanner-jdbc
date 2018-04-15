package nl.topicus.jdbc.statement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.rpc.Code;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.update.Update;
import nl.topicus.jdbc.AbstractCloudSpannerFetcher;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDriver;
import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;

/**
 * 
 * @author loite
 *
 */
abstract class AbstractCloudSpannerStatement extends AbstractCloudSpannerFetcher implements Statement
{
	protected static final String NO_MUTATIONS_IN_READ_ONLY_MODE_EXCEPTION = "The connection is in read-only mode. Mutations are not allowed.";

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
		if (sql.matches("(?is)\\s*INSERT\\s+.*\\s+ON\\s+DUPLICATE\\s+KEY\\s+UPDATE\\s*"))
		{
			sql = sql + " FOO=BAR";
		}
		// Remove @{FORCE_INDEX...} statements
		sql = sql.replaceAll("(?is)\\@\\{\\s*FORCE_INDEX.*\\}", "");

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
	 *             if a database exception occurs while getting the table meta
	 *             data or if the statement tries to update the primary key
	 *             value
	 */
	protected String createInsertSelectOnDuplicateKeyUpdateStatement(Update update) throws SQLException
	{
		String tableName = unquoteIdentifier(update.getTables().get(0).getName());
		TableKeyMetaData table = getConnection().getTable(tableName);
		List<String> keyColumns = table.getKeyColumns();
		List<String> updateColumns = update.getColumns().stream().map(Column::getColumnName).map(String::toUpperCase)
				.collect(Collectors.toList());
		List<String> quotedKeyColumns = keyColumns.stream().map(this::quoteIdentifier).collect(Collectors.toList());
		List<String> quotedAndQualifiedKeyColumns = keyColumns.stream()
				.map(x -> quoteIdentifier(tableName) + "." + quoteIdentifier(x)).collect(Collectors.toList());

		List<String> quotedUpdateColumns = updateColumns.stream().map(this::quoteIdentifier)
				.collect(Collectors.toList());
		List<String> expressions = update.getExpressions().stream().map(Object::toString).collect(Collectors.toList());
		if (updateColumns.stream().anyMatch(keyColumns::contains))
		{
			String invalidCols = updateColumns.stream().filter(keyColumns::contains).collect(Collectors.joining());
			throw new CloudSpannerSQLException(
					"UPDATE of a primary key value is not allowed, cannot UPDATE the column(s) " + invalidCols,
					Code.INVALID_ARGUMENT);
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

	protected String unquoteIdentifier(String identifier)
	{
		return CloudSpannerDriver.unquoteIdentifier(identifier);
	}

	/**
	 * Determines whether the given sql statement must be executed in a single
	 * use read context. This must be done for queries against the information
	 * schema. This method sets the <code>forceSingleUseReadContext</code> to
	 * true if necessary.
	 * 
	 * @param select
	 *            The sql statement to be examined.
	 */
	protected void determineForceSingleUseReadContext(Select select)
	{
		if (select.getSelectBody() != null)
		{
			select.getSelectBody().accept(new SelectVisitorAdapter()
			{
				@Override
				public void visit(PlainSelect plainSelect)
				{
					if (plainSelect.getFromItem() != null)
					{
						plainSelect.getFromItem().accept(new FromItemVisitorAdapter()
						{
							@Override
							public void visit(Table table)
							{
								if (table.getSchemaName() != null
										&& table.getSchemaName().equalsIgnoreCase("INFORMATION_SCHEMA"))
								{
									setForceSingleUseReadContext(true);
								}
							}
						});
					}
				}

			});
		}
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

	protected List<Partition> partitionQuery(com.google.cloud.spanner.Statement statement)
	{
		PartitionOptions po = PartitionOptions.getDefaultInstance();
		return connection.getTransaction().partitionQuery(po, statement);
	}

	protected BatchReadOnlyTransaction getBatchReadOnlyTransaction()
	{
		return connection.getTransaction().getBatchReadOnlyTransaction();
	}

	protected long writeMutations(Mutations mutations) throws SQLException
	{
		if (connection.isReadOnly())
		{
			throw new CloudSpannerSQLException(NO_MUTATIONS_IN_READ_ONLY_MODE_EXCEPTION, Code.FAILED_PRECONDITION);
		}
		if (mutations.isWorker())
		{
			ConversionResult result = mutations.getWorker().call();
			if (result.getException() != null)
			{
				if (result.getException() instanceof SQLException)
					throw (SQLException) result.getException();
				if (result.getException() instanceof SpannerException)
					throw new CloudSpannerSQLException((SpannerException) result.getException());
				throw new CloudSpannerSQLException(result.getException().getMessage(), Code.UNKNOWN,
						result.getException());
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
			throw new CloudSpannerSQLException("Statement is closed", Code.FAILED_PRECONDITION);
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
		checkClosed();
		// noop
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
	public CloudSpannerConnection getConnection() throws SQLException
	{
		return connection;
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
