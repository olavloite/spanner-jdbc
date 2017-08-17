package nl.topicus.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessControlException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Arrays;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerOptions.Builder;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;

import nl.topicus.jdbc.statement.CloudSpannerPreparedStatement;
import nl.topicus.jdbc.statement.CloudSpannerStatement;
import nl.topicus.jdbc.transaction.CloudSpannerTransaction;

/**
 * JDBC Driver for Google Cloud Spanner.
 * 
 * @author loite
 *
 */
public class CloudSpannerConnection extends AbstractCloudSpannerConnection
{
	private final CloudSpannerDriver driver;

	private Spanner spanner;

	private String clientId;

	private DatabaseClient dbClient;

	private DatabaseAdminClient adminClient;

	private boolean autoCommit = true;

	private boolean closed;

	private boolean readOnly;

	private final String url;

	private String simulateProductName;

	private String instanceId;

	private String database;

	private CloudSpannerTransaction transaction;

	CloudSpannerConnection(CloudSpannerDriver driver, String url, String projectId, String instanceId, String database,
			String credentialsPath, String oauthToken) throws SQLException
	{
		this.driver = driver;
		this.instanceId = instanceId;
		this.database = database;
		this.url = url;
		try
		{
			Builder builder = SpannerOptions.newBuilder();
			if (projectId != null)
				builder.setProjectId(projectId);
			GoogleCredentials credentials = null;
			if (credentialsPath != null)
			{
				credentials = getCredentialsFromFile(credentialsPath);
				builder.setCredentials(credentials);
			}
			else if (oauthToken != null)
			{
				credentials = getCredentialsFromOAuthToken(oauthToken);
				builder.setCredentials(credentials);
			}
			if (credentials != null)
			{
				if (credentials instanceof UserCredentials)
				{
					clientId = ((UserCredentials) credentials).getClientId();
				}
				if (credentials instanceof ServiceAccountCredentials)
				{
					clientId = ((ServiceAccountCredentials) credentials).getClientId();
				}
			}

			SpannerOptions options = builder.build();
			spanner = options.getService();
			dbClient = spanner.getDatabaseClient(DatabaseId.of(options.getProjectId(), instanceId, database));
			adminClient = spanner.getDatabaseAdminClient();
			transaction = new CloudSpannerTransaction(dbClient, this);
		}
		catch (Exception e)
		{
			throw new SQLException("Error when opening Google Cloud Spanner connection: " + e.getMessage(), e);
		}
	}

	public static GoogleCredentials getCredentialsFromOAuthToken(String oauthToken) throws IOException
	{
		GoogleCredentials credentials = null;
		if (oauthToken != null && oauthToken.length() > 0)
		{
			credentials = new GoogleCredentials(new AccessToken(oauthToken, null));
		}
		return credentials;
	}

	public static GoogleCredentials getCredentialsFromFile(String credentialsPath) throws IOException
	{
		// First try the environment variable
		GoogleCredentials credentials = null;
		if (credentialsPath != null && credentialsPath.length() > 0)
		{
			InputStream credentialsStream = null;
			try
			{
				File credentialsFile = new File(credentialsPath);
				if (!credentialsFile.isFile())
				{
					// Path will be put in the message from the catch block
					// below
					throw new IOException("File does not exist.");
				}
				credentialsStream = new FileInputStream(credentialsFile);
				credentials = GoogleCredentials.fromStream(credentialsStream,
						CloudSpannerOAuthUtil.HTTP_TRANSPORT_FACTORY);
			}
			catch (IOException e)
			{
				throw new IOException(
						String.format("Error reading credential file %s: %s", credentialsPath, e.getMessage()), e);
			}
			catch (AccessControlException expected)
			{
				// Exception querying file system is expected on App-Engine
			}
			finally
			{
				if (credentialsStream != null)
				{
					credentialsStream.close();
				}
			}
		}
		return credentials;
	}

	Spanner getSpanner()
	{
		return spanner;
	}

	public void setSimulateProductName(String productName)
	{
		this.simulateProductName = productName;
	}

	public Void executeDDL(String sql) throws SQLException
	{
		try
		{
			Operation<Void, UpdateDatabaseDdlMetadata> operation = adminClient.updateDatabaseDdl(instanceId, database,
					Arrays.asList(sql), null);
			operation = operation.waitFor();
			return operation.getResult();
		}
		catch (SpannerException e)
		{
			throw new SQLException("Could not execute DDL statement: " + e.getLocalizedMessage(), e);
		}
	}

	String getProductName()
	{
		if (simulateProductName != null)
			return simulateProductName;
		return "Google Cloud Spanner";
	}

	@Override
	public CloudSpannerStatement createStatement() throws SQLException
	{
		return new CloudSpannerStatement(this, dbClient);
	}

	@Override
	public CloudSpannerPreparedStatement prepareStatement(String sql) throws SQLException
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
		transaction.commit();
	}

	@Override
	public void rollback() throws SQLException
	{
		transaction.rollback();
	}

	public CloudSpannerTransaction getTransaction()
	{
		return transaction;
	}

	@Override
	public void close() throws SQLException
	{
		transaction.rollback();
		closed = true;
		driver.closeConnection(this);
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return closed;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException
	{
		return new CloudSpannerDatabaseMetaData(this);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException
	{
		if (transaction.isRunning())
			throw new SQLException(
					"There is currently a transaction running. Commit or rollback the running transaction before changing read-only mode.");
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

	public String getClientId()
	{
		return clientId;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException
	{
		Statement statement = createStatement();
		statement.setQueryTimeout(timeout);
		try (ResultSet rs = statement.executeQuery("SELECT 1"))
		{
			if (rs.next())
				return true;
		}
		return false;
	}

}
