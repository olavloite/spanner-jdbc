package nl.topicus.jdbc.transaction;

import java.sql.SQLException;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options.QueryOption;
import com.google.cloud.spanner.Options.ReadOption;
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

	private DatabaseClient dbClient;

	public CloudSpannerTransaction(DatabaseClient dbClient)
	{
		this.dbClient = dbClient;
	}

	public void begin()
	{
		if (transactionThread == null)
		{
			transactionThread = new TransactionThread(dbClient);
			transactionThread.start();
		}
	}

	public void commit() throws SQLException
	{
		if (transactionThread != null)
		{
			transactionThread.commit();
			transactionThread = null;
		}
	}

	public void rollback() throws SQLException
	{
		if (transactionThread != null)
		{
			transactionThread.rollback();
			transactionThread = null;
		}
	}

	private void checkTransaction()
	{
		if (transactionThread == null)
			begin();
	}

	@Override
	public void buffer(Mutation mutation)
	{
		checkTransaction();
		transactionThread.buffer(mutation);
	}

	@Override
	public void buffer(Iterable<Mutation> mutations)
	{
		checkTransaction();
		transactionThread.buffer(mutations);
	}

	@Override
	public ResultSet executeQuery(Statement statement, QueryOption... options)
	{
		checkTransaction();
		return transactionThread.executeQuery(statement);
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
