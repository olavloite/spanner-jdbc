package nl.topicus.jdbc.transaction;

import java.sql.SQLException;

import nl.topicus.jdbc.CloudSpannerConnection;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options.QueryOption;
import com.google.cloud.spanner.Options.ReadOption;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;

/**
 * An abstraction of transactions on Google Cloud Spanner JDBC connections.
 * 
 * @author loite
 *
 */
public class CloudSpannerTransaction implements TransactionContext
{
	private TransactionThread transactionThread;

	private ReadOnlyTransaction readOnlyTransaction;

	private DatabaseClient dbClient;

	private CloudSpannerConnection connection;

	public CloudSpannerTransaction(DatabaseClient dbClient, CloudSpannerConnection connection)
	{
		this.dbClient = dbClient;
		this.connection = connection;
	}

	public boolean isRunning()
	{
		return readOnlyTransaction != null || transactionThread != null;
	}

	public void begin() throws SQLException
	{
		if (connection.isReadOnly())
		{
			if (readOnlyTransaction == null)
			{
				readOnlyTransaction = dbClient.readOnlyTransaction();
			}
		}
		else
		{
			if (transactionThread == null)
			{
				transactionThread = new TransactionThread(dbClient);
				transactionThread.start();
			}
		}
	}

	public void commit() throws SQLException
	{
		if (connection.isReadOnly())
		{
			if (readOnlyTransaction != null)
			{
				readOnlyTransaction.close();
				readOnlyTransaction = null;
			}
		}
		else
		{
			if (transactionThread != null)
			{
				transactionThread.commit();
				transactionThread = null;
			}
		}
	}

	public void rollback() throws SQLException
	{
		if (connection.isReadOnly())
		{
			if (readOnlyTransaction != null)
			{
				readOnlyTransaction.close();
				readOnlyTransaction = null;
			}
		}
		else
		{
			if (transactionThread != null)
			{
				transactionThread.rollback();
				transactionThread = null;
			}
		}
	}

	private void checkTransaction()
	{
		if (transactionThread == null && readOnlyTransaction == null)
		{
			try
			{
				begin();
			}
			catch (SQLException e)
			{
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void buffer(Mutation mutation)
	{
		checkTransaction();
		if (transactionThread == null)
			throw new IllegalStateException("Mutations are not allowed in read-only mode");
		transactionThread.buffer(mutation);
	}

	@Override
	public void buffer(Iterable<Mutation> mutations)
	{
		checkTransaction();
		if (transactionThread == null)
			throw new IllegalStateException("Mutations are not allowed in read-only mode");
		transactionThread.buffer(mutations);
	}

	@Override
	public ResultSet executeQuery(Statement statement, QueryOption... options)
	{
		checkTransaction();
		if (readOnlyTransaction != null)
			return readOnlyTransaction.executeQuery(statement, options);
		else if (transactionThread != null)
			return transactionThread.executeQuery(statement);

		throw new IllegalStateException("No transaction found (this should not happen)");
	}

	@Override
	public ResultSet read(String table, KeySet keys, Iterable<String> columns, ReadOption... options)
	{
		return null;
	}

	@Override
	public ResultSet readUsingIndex(String table, String index, KeySet keys, Iterable<String> columns,
			ReadOption... options)
	{
		return null;
	}

	@Override
	public Struct readRow(String table, Key key, Iterable<String> columns)
	{
		return null;
	}

	@Override
	public Struct readRowUsingIndex(String table, String index, Key key, Iterable<String> columns)
	{
		return null;
	}

	@Override
	public ResultSet analyzeQuery(Statement statement, QueryAnalyzeMode queryMode)
	{
		return null;
	}

	@Override
	public void close()
	{
	}

}
