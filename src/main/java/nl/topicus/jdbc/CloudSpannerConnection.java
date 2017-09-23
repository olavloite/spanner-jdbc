package nl.topicus.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Arrays;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

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

import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;
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

	private MetaDataStore metaDataStore;

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
			metaDataStore = new MetaDataStore(this);
		}
		catch (Exception e)
		{
			throw new SQLException("Error when opening Google Cloud Spanner connection: " + e.getMessage(), e);
		}
	}

	public static GoogleCredentials getCredentialsFromOAuthToken(String oauthToken)
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
		if (credentialsPath == null || credentialsPath.length() == 0)
			throw new IllegalArgumentException("credentialsPath may not be null or empty");
		GoogleCredentials credentials = null;
		File credentialsFile = new File(credentialsPath);
		if (!credentialsFile.isFile())
		{
			throw new IOException(
					String.format("Error reading credential file %s: File does not exist", credentialsPath));
		}
		try (InputStream credentialsStream = new FileInputStream(credentialsFile))
		{
			credentials = GoogleCredentials.fromStream(credentialsStream, CloudSpannerOAuthUtil.HTTP_TRANSPORT_FACTORY);
		}
		return credentials;
	}

	public static String getServiceAccountProjectId(String credentialsPath)
	{
		String project = null;
		if (credentialsPath != null)
		{
			try (InputStream credentialsStream = new FileInputStream(credentialsPath))
			{
				JSONObject json = new JSONObject(new JSONTokener(credentialsStream));
				project = json.getString("project_id");
			}
			catch (IOException | JSONException ex)
			{
				// ignore
			}
		}
		return project;
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
			throw new SQLException("Could not execute DDL statement " + sql + ": " + e.getLocalizedMessage(), e);
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

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException
	{
		return CloudSpannerArray.createArray(typeName, elements);
	}

	public TableKeyMetaData getTable(String name) throws SQLException
	{
		return metaDataStore.getTable(name);
	}

}
