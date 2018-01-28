package nl.topicus.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerOptions.Builder;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import com.google.rpc.Code;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;

import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.resultset.CloudSpannerResultSet;
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
	public static class CloudSpannerDatabaseSpecification
	{
		public final String project;

		public final String instance;

		public final String database;

		public CloudSpannerDatabaseSpecification(String instance, String database)
		{
			this(null, instance, database);
		}

		public CloudSpannerDatabaseSpecification(String project, String instance, String database)
		{
			this.project = project;
			this.instance = instance;
			this.database = database;
		}
	}

	private final CloudSpannerDriver driver;

	private final CloudSpannerDatabaseSpecification database;

	private Spanner spanner;

	private String clientId;

	private DatabaseClient dbClient;

	private DatabaseAdminClient adminClient;

	private boolean autoCommit = true;

	private boolean closed;

	private boolean readOnly;

	private final RunningOperationsStore operations = new RunningOperationsStore();

	private final String url;

	private final Properties suppliedProperties;

	private boolean originalAllowExtendedMode;
	private boolean allowExtendedMode;

	private boolean originalAsyncDdlOperations;
	private boolean asyncDdlOperations;

	private boolean originalAutoBatchDdlOperations;
	private boolean autoBatchDdlOperations;
	private List<String> autoBatchedDdlOperations = new ArrayList<>();

	private boolean originalReportDefaultSchemaAsNull = true;
	private boolean reportDefaultSchemaAsNull = true;

	private String simulateProductName;
	private Integer simulateMajorVersion;
	private Integer simulateMinorVersion;

	private CloudSpannerTransaction transaction;

	private Timestamp lastCommitTimestamp;

	private MetaDataStore metaDataStore;

	private static int nextConnectionID = 1;

	private final Logger logger;

	private Map<String, Class<?>> typeMap = new HashMap<>();

	CloudSpannerConnection(CloudSpannerDriver driver, String url, CloudSpannerDatabaseSpecification database,
			String credentialsPath, String oauthToken, Properties suppliedProperties) throws SQLException
	{
		this.driver = driver;
		this.database = database;
		this.url = url;
		this.suppliedProperties = suppliedProperties;

		int logLevel = CloudSpannerDriver.getLogLevel();
		synchronized (CloudSpannerConnection.class)
		{
			logger = new Logger(nextConnectionID++);
			logger.setLogLevel(logLevel);
		}

		try
		{
			Builder builder = SpannerOptions.newBuilder();
			if (database.project != null)
				builder.setProjectId(database.project);
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
			dbClient = spanner
					.getDatabaseClient(DatabaseId.of(options.getProjectId(), database.instance, database.database));
			adminClient = spanner.getDatabaseAdminClient();
			transaction = new CloudSpannerTransaction(dbClient, this);
			metaDataStore = new MetaDataStore(this);
		}
		catch (SpannerException e)
		{
			throw new CloudSpannerSQLException("Error when opening Google Cloud Spanner connection: " + e.getMessage(),
					e);
		}
		catch (IOException e)
		{
			throw new CloudSpannerSQLException("Error when opening Google Cloud Spanner connection: " + e.getMessage(),
					Code.UNKNOWN, e);
		}
	}

	public static GoogleCredentials getCredentialsFromOAuthToken(String oauthToken)
	{
		GoogleCredentials credentials = null;
		if (oauthToken != null && oauthToken.length() > 0)
		{
			credentials = GoogleCredentials.create(new AccessToken(oauthToken, null));
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

	public String getSimulateProductName()
	{
		return simulateProductName;
	}

	@Override
	public void setSimulateProductName(String productName)
	{
		this.simulateProductName = productName;
	}

	public Integer getSimulateMajorVersion()
	{
		return simulateMajorVersion;
	}

	@Override
	public void setSimulateMajorVersion(Integer majorVersion)
	{
		this.simulateMajorVersion = majorVersion;
	}

	public Integer getSimulateMinorVersion()
	{
		return simulateMinorVersion;
	}

	@Override
	public void setSimulateMinorVersion(Integer minorVersion)
	{
		this.simulateMinorVersion = minorVersion;
	}

	/**
	 * Execute one or more DDL-statements on the database and wait for it to
	 * finish or return after syntax check (when running in async mode). Calling
	 * this method will also automatically commit the currently running
	 * transaction.
	 * 
	 * @param sql
	 *            The DDL-statement(s) to execute
	 * @return Nothing
	 * @throws SQLException
	 *             If an error occurs during the execution of the statement.
	 */
	public Void executeDDL(List<String> sql) throws SQLException
	{
		if (!getAutoCommit())
			commit();
		try
		{
			Operation<Void, UpdateDatabaseDdlMetadata> operation = adminClient.updateDatabaseDdl(database.instance,
					database.database, sql, null);
			if (asyncDdlOperations)
			{
				operations.addOperation(sql, operation);
			}
			else
			{
				do
				{
					operation = operation.waitFor();
				}
				while (!operation.isDone());
			}
			return operation.getResult();
		}
		catch (SpannerException e)
		{
			throw new CloudSpannerSQLException(
					"Could not execute DDL statement(s) " + String.join("\n;\n", sql) + ": " + e.getMessage(), e);
		}
	}

	/**
	 * Clears the asynchronous DDL-operations that have finished.
	 * 
	 * @return The number of operations that were cleared
	 */
	public int clearFinishedDDLOperations()
	{
		return operations.clearFinishedOperations();
	}

	/**
	 * Waits for all asynchronous DDL-operations that have been issued by this
	 * connection to finish.
	 * 
	 * @throws SQLException
	 *             If a database exception occurs while waiting for the
	 *             operations to finish
	 * 
	 */
	public void waitForDdlOperations() throws SQLException
	{
		operations.waitForOperations();
	}

	/**
	 * Returns a ResultSet containing all asynchronous DDL-operations started by
	 * this connection. It does not contain DDL-operations that have been
	 * started by other connections or by other means.
	 * 
	 * @param statement
	 *            The statement that requested the operations
	 * @return A ResultSet with the DDL-operations
	 */
	public ResultSet getRunningDDLOperations(Statement statement)
	{
		return operations.getOperations(statement);
	}

	@Override
	public String getProductName()
	{
		if (getSimulateProductName() != null)
			return getSimulateProductName();
		return "Google Cloud Spanner";
	}

	@Override
	public CloudSpannerStatement createStatement() throws SQLException
	{
		checkClosed();
		return new CloudSpannerStatement(this, dbClient);
	}

	@Override
	public CloudSpannerPreparedStatement prepareStatement(String sql) throws SQLException
	{
		checkClosed();
		return new CloudSpannerPreparedStatement(sql, this, dbClient);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String nativeSQL(String sql) throws SQLException
	{
		checkClosed();
		return sql;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException
	{
		checkClosed();
		this.autoCommit = autoCommit;
	}

	@Override
	public boolean getAutoCommit() throws SQLException
	{
		checkClosed();
		return autoCommit;
	}

	@Override
	public void commit() throws SQLException
	{
		checkClosed();
		lastCommitTimestamp = transaction.commit();
	}

	@Override
	public void rollback() throws SQLException
	{
		checkClosed();
		transaction.rollback();
	}

	public CloudSpannerTransaction getTransaction()
	{
		return transaction;
	}

	@Override
	public void close() throws SQLException
	{
		if (closed)
			return;
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
	public CloudSpannerDatabaseMetaData getMetaData() throws SQLException
	{
		checkClosed();
		return new CloudSpannerDatabaseMetaData(this);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException
	{
		checkClosed();
		if (transaction.isRunning())
			throw new CloudSpannerSQLException(
					"There is currently a transaction running. Commit or rollback the running transaction before changing read-only mode.",
					Code.FAILED_PRECONDITION);
		this.readOnly = readOnly;
	}

	@Override
	public boolean isReadOnly() throws SQLException
	{
		checkClosed();
		return readOnly;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException
	{
		checkClosed();
		if (level != Connection.TRANSACTION_SERIALIZABLE)
		{
			throw new CloudSpannerSQLException(
					"Transaction level " + level
							+ " is not supported. Only Connection.TRANSACTION_SERIALIZABLE is supported",
					Code.INVALID_ARGUMENT);
		}
	}

	@Override
	public int getTransactionIsolation() throws SQLException
	{
		checkClosed();
		return Connection.TRANSACTION_SERIALIZABLE;
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
	{
		checkClosed();
		return new CloudSpannerStatement(this, dbClient);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException
	{
		checkClosed();
		return new CloudSpannerPreparedStatement(sql, this, dbClient);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException
	{
		checkClosed();
		return new CloudSpannerStatement(this, dbClient);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException
	{
		checkClosed();
		return new CloudSpannerPreparedStatement(sql, this, dbClient);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
	{
		checkClosed();
		return new CloudSpannerPreparedStatement(sql, this, dbClient);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
	{
		checkClosed();
		return new CloudSpannerPreparedStatement(sql, this, dbClient);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
	{
		checkClosed();
		return new CloudSpannerPreparedStatement(sql, this, dbClient);
	}

	@Override
	public String getUrl()
	{
		return url;
	}

	@Override
	public String getClientId()
	{
		return clientId;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException
	{
		if (isClosed())
			return false;
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
	public CloudSpannerArray createArrayOf(String typeName, Object[] elements) throws SQLException
	{
		checkClosed();
		return CloudSpannerArray.createArray(typeName, elements);
	}

	public TableKeyMetaData getTable(String name) throws SQLException
	{
		return metaDataStore.getTable(name);
	}

	@Override
	public Properties getSuppliedProperties()
	{
		return suppliedProperties;
	}

	@Override
	public boolean isAllowExtendedMode()
	{
		return allowExtendedMode;
	}

	@Override
	public int setAllowExtendedMode(boolean allowExtendedMode)
	{
		this.allowExtendedMode = allowExtendedMode;
		return 1;
	}

	boolean isOriginalAllowExtendedMode()
	{
		return originalAllowExtendedMode;
	}

	void setOriginalAllowExtendedMode(boolean allowExtendedMode)
	{
		this.originalAllowExtendedMode = allowExtendedMode;
	}

	@Override
	public boolean isAsyncDdlOperations()
	{
		return asyncDdlOperations;
	}

	@Override
	public int setAsyncDdlOperations(boolean asyncDdlOperations)
	{
		this.asyncDdlOperations = asyncDdlOperations;
		return 1;
	}

	boolean isOriginalAsyncDdlOperations()
	{
		return originalAsyncDdlOperations;
	}

	void setOriginalAsyncDdlOperations(boolean asyncDdlOperations)
	{
		this.originalAsyncDdlOperations = asyncDdlOperations;
	}

	@Override
	public boolean isAutoBatchDdlOperations()
	{
		return autoBatchDdlOperations;
	}

	@Override
	public int setAutoBatchDdlOperations(boolean autoBatchDdlOperations)
	{
		clearAutoBatchedDdlOperations();
		this.autoBatchDdlOperations = autoBatchDdlOperations;
		return 1;
	}

	boolean isOriginalAutoBatchDdlOperations()
	{
		return originalAutoBatchDdlOperations;
	}

	void setOriginalAutoBatchDdlOperations(boolean autoBatchDdlOperations)
	{
		this.originalAutoBatchDdlOperations = autoBatchDdlOperations;
	}

	@Override
	public boolean isReportDefaultSchemaAsNull()
	{
		return reportDefaultSchemaAsNull;
	}

	@Override
	public int setReportDefaultSchemaAsNull(boolean reportDefaultSchemaAsNull)
	{
		this.reportDefaultSchemaAsNull = reportDefaultSchemaAsNull;
		return 1;
	}

	boolean isOriginalReportDefaultSchemaAsNull()
	{
		return originalReportDefaultSchemaAsNull;
	}

	void setOriginalReportDefaultSchemaAsNull(boolean reportDefaultSchemaAsNull)
	{
		this.originalReportDefaultSchemaAsNull = reportDefaultSchemaAsNull;
	}

	/**
	 * Set a dynamic connection property, such as AsyncDdlOperations
	 * 
	 * @param propertyName
	 *            The name of the dynamic connection property
	 * @param propertyValue
	 *            The value to set
	 * @return 1 if the property was set, 0 if not (this complies with the
	 *         normal behaviour of executeUpdate(...) methods)
	 */
	public int setDynamicConnectionProperty(String propertyName, String propertyValue)
	{
		return getPropertySetter(propertyName).apply(Boolean.valueOf(propertyValue));
	}

	/**
	 * Reset a dynamic connection property to its original value, such as
	 * AsyncDdlOperations
	 * 
	 * @param propertyName
	 *            The name of the dynamic connection property
	 * @return 1 if the property was reset, 0 if not (this complies with the
	 *         normal behaviour of executeUpdate(...) methods)
	 */
	public int resetDynamicConnectionProperty(String propertyName)
	{
		return getPropertySetter(propertyName).apply(getOriginalValueGetter(propertyName).get());
	}

	private Supplier<Boolean> getOriginalValueGetter(String propertyName)
	{
		if (propertyName.equalsIgnoreCase(ConnectionProperties
				.getPropertyName(ConnectionProperties.ALLOW_EXTENDED_MODE)))
		{
			return this::isOriginalAllowExtendedMode;
		}
		if (propertyName.equalsIgnoreCase(ConnectionProperties
				.getPropertyName(ConnectionProperties.ASYNC_DDL_OPERATIONS)))
		{
			return this::isOriginalAsyncDdlOperations;
		}
		if (propertyName.equalsIgnoreCase(ConnectionProperties
				.getPropertyName(ConnectionProperties.AUTO_BATCH_DDL_OPERATIONS)))
		{
			return this::isOriginalAutoBatchDdlOperations;
		}
		if (propertyName.equalsIgnoreCase(ConnectionProperties
				.getPropertyName(ConnectionProperties.REPORT_DEFAULT_SCHEMA_AS_NULL)))
		{
			return this::isOriginalReportDefaultSchemaAsNull;
		}
		// Return a no-op to avoid null checks
		return () -> false;
	}

	private Function<Boolean, Integer> getPropertySetter(String propertyName)
	{
		if (propertyName.equalsIgnoreCase(ConnectionProperties
				.getPropertyName(ConnectionProperties.ALLOW_EXTENDED_MODE)))
		{
			return this::setAllowExtendedMode;
		}
		if (propertyName.equalsIgnoreCase(ConnectionProperties
				.getPropertyName(ConnectionProperties.ASYNC_DDL_OPERATIONS)))
		{
			return this::setAsyncDdlOperations;
		}
		if (propertyName.equalsIgnoreCase(ConnectionProperties
				.getPropertyName(ConnectionProperties.AUTO_BATCH_DDL_OPERATIONS)))
		{
			return this::setAutoBatchDdlOperations;
		}
		if (propertyName.equalsIgnoreCase(ConnectionProperties
				.getPropertyName(ConnectionProperties.REPORT_DEFAULT_SCHEMA_AS_NULL)))
		{
			return this::setReportDefaultSchemaAsNull;
		}
		// Return a no-op to avoid null checks
		return x -> 0;
	}

	public ResultSet getDynamicConnectionProperties(Statement statement)
	{
		return getDynamicConnectionProperty(statement, null);
	}

	public ResultSet getDynamicConnectionProperty(Statement statement, String propertyName)
	{
		Map<String, String> values = new HashMap<>();
		if (propertyName == null || propertyName.equalsIgnoreCase(ConnectionProperties
				.getPropertyName(ConnectionProperties.ALLOW_EXTENDED_MODE)))
		{
			values.put(
					ConnectionProperties
							.getPropertyName(ConnectionProperties.ALLOW_EXTENDED_MODE),
					String.valueOf(isAllowExtendedMode()));
		}
		if (propertyName == null || propertyName.equalsIgnoreCase(ConnectionProperties
				.getPropertyName(ConnectionProperties.ASYNC_DDL_OPERATIONS)))
		{
			values.put(
					ConnectionProperties
							.getPropertyName(ConnectionProperties.ASYNC_DDL_OPERATIONS),
					String.valueOf(isAsyncDdlOperations()));
		}
		if (propertyName == null || propertyName.equalsIgnoreCase(ConnectionProperties
				.getPropertyName(ConnectionProperties.AUTO_BATCH_DDL_OPERATIONS)))
		{
			values.put(
					ConnectionProperties
							.getPropertyName(ConnectionProperties.AUTO_BATCH_DDL_OPERATIONS),
					String.valueOf(isAutoBatchDdlOperations()));
		}
		if (propertyName == null || propertyName.equalsIgnoreCase(ConnectionProperties
				.getPropertyName(ConnectionProperties.REPORT_DEFAULT_SCHEMA_AS_NULL)))
		{
			values.put(
					ConnectionProperties
							.getPropertyName(ConnectionProperties.REPORT_DEFAULT_SCHEMA_AS_NULL),
					String.valueOf(isReportDefaultSchemaAsNull()));
		}
		return createResultSet(statement, values);
	}

	private ResultSet createResultSet(Statement statement, Map<String, String> values)
	{
		List<Struct> rows = new ArrayList<>(values.size());
		for (Entry<String, String> entry : values.entrySet())
		{
			rows.add(Struct.newBuilder().add("NAME", Value.string(entry.getKey()))
					.add("VALUE", Value.string(entry.getValue())).build());
		}
		com.google.cloud.spanner.ResultSet rs = ResultSets.forRows(
				Type.struct(StructField.of("NAME", Type.string()), StructField.of("VALUE", Type.string())), rows);
		return new CloudSpannerResultSet(statement, rs);
	}

	/**
	 * 
	 * @return The commit timestamp of the last transaction that committed
	 *         succesfully
	 */
	@Override
	public Timestamp getLastCommitTimestamp()
	{
		return lastCommitTimestamp;
	}

	/**
	 * 
	 * @return A new connection with the same URL and properties as this
	 *         connection. You can use this method if you want to open a new
	 *         connection to the same database, for example to run a number of
	 *         statements in a different transaction than the transaction you
	 *         are currently using on this connection.
	 * @throws SQLException
	 *             If an error occurs while opening the new connection
	 */
	public CloudSpannerConnection createCopyConnection() throws SQLException
	{
		return (CloudSpannerConnection) DriverManager.getConnection(getUrl(), getSuppliedProperties());
	}

	public Logger getLogger()
	{
		return logger;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException
	{
		checkClosed();
		return typeMap;
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException
	{
		checkClosed();
		this.typeMap = map;
	}

	/**
	 * 
	 * @return The number of nodes of this Cloud Spanner instance
	 * @throws SQLException
	 *             If an exception occurs when trying to get the number of nodes
	 */
	public int getNodeCount() throws SQLException
	{
		try
		{
			if (database != null && database.instance != null)
			{
				Instance instance = getSpanner().getInstanceAdminClient().getInstance(database.instance);
				return instance == null ? 0 : instance.getNodeCount();
			}
			return 0;
		}
		catch (SpannerException e)
		{
			throw new CloudSpannerSQLException(e);
		}
	}

	/**
	 * Prepare the current transaction by writing the mutations to the
	 * XA_TRANSACTIONS table instead of persisting them in the actual tables.
	 * 
	 * @param xid
	 *            The id of the prepared transaction
	 * @throws SQLException
	 *             If an exception occurs while saving the mutations to the
	 *             database for later commit
	 */
	public void prepareTransaction(String xid) throws SQLException
	{
		transaction.prepareTransaction(xid);
	}

	/**
	 * Commit a previously prepared transaction.
	 * 
	 * @param xid
	 *            The id of the prepared transaction
	 * @throws SQLException
	 *             If an error occurs when writing the mutations to the database
	 */
	public void commitPreparedTransaction(String xid) throws SQLException
	{
		transaction.commitPreparedTransaction(xid);
	}

	/**
	 * Rollback a previously prepared transaction.
	 * 
	 * @param xid
	 *            The id of the prepared transaction to rollback
	 * @throws SQLException
	 *             If an error occurs while rolling back the prepared
	 *             transaction
	 */
	public void rollbackPreparedTransaction(String xid) throws SQLException
	{
		transaction.rollbackPreparedTransaction(xid);
	}

	public List<String> getAutoBatchedDdlOperations()
	{
		return Collections.unmodifiableList(autoBatchedDdlOperations);
	}

	public void clearAutoBatchedDdlOperations()
	{
		autoBatchedDdlOperations.clear();
	}

	public void addAutoBatchedDdlOperation(String sql)
	{
		autoBatchedDdlOperations.add(sql);
	}

}
