package nl.topicus.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import nl.topicus.jdbc.statement.CloudSpannerPreparedStatement;
import nl.topicus.jdbc.statement.CloudSpannerStatement;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerOptions.Builder;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;

/**
 * JDBC Driver for Google Cloud Spanner.
 * 
 * @author loite
 *
 */
public class CloudSpannerConnection extends AbstractCloudSpannerConnection
{
	private DatabaseClient dbClient;

	private boolean autoCommit;

	private boolean closed;

	private boolean readOnly;

	private final String url;

	private String simulateProductName;

	CloudSpannerConnection(String projectId, String instanceId, String database) throws SQLException
	{
		try
		{
			Builder builder = SpannerOptions.newBuilder();
			if (projectId != null)
				builder.setProjectId(projectId);

			SpannerOptions options = builder.build();
			Spanner spanner = options.getService();
			dbClient = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId(), instanceId, database));
		}
		catch (Exception e)
		{
			throw new SQLException("Error when opening Google Cloud Spanner connection", e);
		}
		url = "jdbc:cloudspanner://localhost;Project=" + projectId + ";Instance=" + instanceId + ";Database="
				+ database;
	}

	public void setSimulateProductName(String productName)
	{
		this.simulateProductName = productName;
	}

	String getProductName()
	{
		if (simulateProductName != null)
			return simulateProductName;
		return "Google Cloud Spanner";
	}

	@Override
	public Statement createStatement() throws SQLException
	{
		return new CloudSpannerStatement(this, dbClient);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException
	{
		return new CloudSpannerPreparedStatement(sql, this, dbClient);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String nativeSQL(String sql) throws SQLException
	{
		return sql;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException
	{
		this.autoCommit = autoCommit;
	}

	@Override
	public boolean getAutoCommit() throws SQLException
	{
		return autoCommit;
	}

	@Override
	public void commit() throws SQLException
	{
		TransactionCallable<Void> callable = new TransactionCallable<Void>()
		{

			@Override
			public Void run(TransactionContext transaction) throws Exception
			{
				// TODO Auto-generated method stub
				return null;
			}
		};
		dbClient.readWriteTransaction().run(callable);
		// TODO Auto-generated method stub

	}

	@Override
	public void rollback() throws SQLException
	{
		// TODO Auto-generated method stub

	}

	protected void startNewTransaction()
	{

	}

	@Override
	public void close() throws SQLException
	{
		// TODO Stop running transaction
		closed = true;
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return closed;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException
	{
		return new CloudSpannerMetaData(this);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException
	{
		this.readOnly = readOnly;
	}

	@Override
	public boolean isReadOnly() throws SQLException
	{
		return readOnly;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException
	{
		if (level != Connection.TRANSACTION_SERIALIZABLE)
		{
			throw new SQLException("Transaction level " + level
					+ " is not supported. Only Connection.TRANSACTION_SERIALIZABLE is supported");
		}
	}

	@Override
	public int getTransactionIsolation() throws SQLException
	{
		return Connection.TRANSACTION_SERIALIZABLE;
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
	{
		return new CloudSpannerStatement(this, dbClient);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException
	{
		return new CloudSpannerPreparedStatement(sql, this, dbClient);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException
	{
		return new CloudSpannerStatement(this, dbClient);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException
	{
		return new CloudSpannerPreparedStatement(sql, this, dbClient);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
	{
		return new CloudSpannerPreparedStatement(sql, this, dbClient);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
	{
		return new CloudSpannerPreparedStatement(sql, this, dbClient);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
	{
		return new CloudSpannerPreparedStatement(sql, this, dbClient);
	}

	public String getUrl()
	{
		return url;
	}

}
