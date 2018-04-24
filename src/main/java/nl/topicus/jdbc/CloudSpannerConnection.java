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
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.google.api.core.InternalApi;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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

		private final String key;

		public CloudSpannerDatabaseSpecification(String instance, String database)
		{
			this(null, instance, database);
		}

		public CloudSpannerDatabaseSpecification(String project, String instance, String database)
		{
			Preconditions.checkNotNull(instance);
			Preconditions.checkNotNull(database);
			this.project = project;
			this.instance = instance;
			this.database = database;
			this.key = Objects.toString(project, "") + "/" + instance + "/" + database;
		}

		@Override
		public int hashCode()
		{
			return key.hashCode();
		}

		@Override
		public boolean equals(Object o)
		{
			if (!(o instanceof CloudSpannerDatabaseSpecification))
				return false;
			return ((CloudSpannerDatabaseSpecification) o).key.equals(key);
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

	/**
	 * Special mode for Cloud Spanner: When the connection is in this mode,
	 * queries will be executed using the {@link BatchClient} instead of the
	 * default {@link DatabaseClient}
	 */
	private boolean originalBatchReadOnly;
	private boolean batchReadOnly;

	private final RunningOperationsStore operations = new RunningOperationsStore();

	private final String url;

	private final Properties suppliedProperties;

	private boolean originalAllowExtendedMode;
	private boolean allowExtendedMode;

	private boolean originalAsyncDdlOperations;
	private boolean asyncDdlOperations;

	private boolean originalAutoBatchDdlOperations;
	private boolean autoBatchDdlOperations;
	private final List<String> autoBatchedDdlOperations = new ArrayList<>();

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

	@VisibleForTesting
	CloudSpannerConnection()
	{
		this(null);
	}

	@VisibleForTesting
	CloudSpannerConnection(DatabaseClient dbClient, BatchClient batchClient)
	{
		this.driver = null;
		this.database = null;
		this.url = null;
		this.suppliedProperties = null;
		this.logger = null;
		this.dbClient = dbClient;
		this.transaction = new CloudSpannerTransaction(dbClient, batchClient, this);
		this.metaDataStore = new MetaDataStore(this);
	}

	@VisibleForTesting
	CloudSpannerConnection(CloudSpannerDatabaseSpecification database)
	{
		this.driver = null;
		this.database = database;
		this.url = null;
		this.suppliedProperties = null;
		this.logger = null;
		this.transaction = new CloudSpannerTransaction(null, null, this);
		this.metaDataStore = new MetaDataStore(this);
	}

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
			Credentials credentials = null;
			if (credentialsPath != null)
			{
				credentials = getCredentialsFromFile(credentialsPath);
			}
			else if (oauthToken != null)
			{
				credentials = getCredentialsFromOAuthToken(oauthToken);
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
			spanner = driver.getSpanner(database.project, credentials);
			dbClient = spanner.getDatabaseClient(
					DatabaseId.of(spanner.getOptions().getProjectId(), database.instance, database.database));
			BatchClient batchClient = spanner.getBatchClient(
					DatabaseId.of(spanner.getOptions().getProjectId(), database.instance, database.database));
			adminClient = spanner.getDatabaseAdminClient();
			transaction = new CloudSpannerTransaction(dbClient, batchClient, this);
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
	 * @param inputSql
	 *            The DDL-statement(s) to execute. Some statements may end up
	 *            not being sent to Cloud Spanner if they contain IF [NOT]
	 *            EXISTS clauses. The driver will check whether the condition is
	 *            met, and only then will it be sent to Cloud Spanner.
	 * @return Nothing
	 * @throws SQLException
	 *             If an error occurs during the execution of the statement.
	 */
	public Void executeDDL(List<String> inputSql) throws SQLException
	{
		if (!getAutoCommit())
			commit();
		// Check for IF [NOT] EXISTS statements
		List<String> sql = getActualSql(inputSql);
		if (!sql.isEmpty())
		{
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
		return null;
	}

	private List<String> getActualSql(List<String> sql) throws SQLException
	{
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		List<String> actualSql = new ArrayList<>(sql.size());
		for (DDLStatement statement : statements)
		{
			if (statement.shouldExecute(this))
			{
				actualSql.add(statement.getSql());
			}
		}
		return actualSql;
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
	public ResultSet getRunningDDLOperations(CloudSpannerStatement statement)
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
		if (autoCommit != this.autoCommit && isBatchReadOnly())
		{
			throw new CloudSpannerSQLException(
					"The connection is currently in batch read-only mode. Please turn off batch read-only before changing auto-commit mode.",
					Code.FAILED_PRECONDITION);
		}
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
		lastCommitTimestamp = getTransaction().commit();
	}

	@Override
	public void rollback() throws SQLException
	{
		checkClosed();
		getTransaction().rollback();
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
		getTransaction().rollback();
		closed = true;
		driver.closeConnection(this);
	}

	@InternalApi
	void markClosed()
	{
		closed = true;
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
		if (readOnly != this.readOnly)
		{
			if (getTransaction().isRunning())
			{
				throw new CloudSpannerSQLException(
						"There is currently a transaction running. Commit or rollback the running transaction before changing read-only mode.",
						Code.FAILED_PRECONDITION);
			}
			if (isBatchReadOnly())
			{
				throw new CloudSpannerSQLException(
						"The connection is currently in batch read-only mode. Please turn off batch read-only before changing read-only mode.",
						Code.FAILED_PRECONDITION);
			}
		}
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
	 * @throws SQLException
	 *             Throws {@link SQLException} if a database error occurs
	 */
	public int setDynamicConnectionProperty(String propertyName, String propertyValue) throws SQLException
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
	 * @throws SQLException
	 *             Throws {@link SQLException} if a database error occurs
	 */
	public int resetDynamicConnectionProperty(String propertyName) throws SQLException
	{
		return getPropertySetter(propertyName).apply(getOriginalValueGetter(propertyName).get());
	}

	private Supplier<Boolean> getOriginalValueGetter(String propertyName)
	{
		if (propertyName
				.equalsIgnoreCase(ConnectionProperties.getPropertyName(ConnectionProperties.ALLOW_EXTENDED_MODE)))
		{
			return this::isOriginalAllowExtendedMode;
		}
		if (propertyName
				.equalsIgnoreCase(ConnectionProperties.getPropertyName(ConnectionProperties.ASYNC_DDL_OPERATIONS)))
		{
			return this::isOriginalAsyncDdlOperations;
		}
		if (propertyName
				.equalsIgnoreCase(ConnectionProperties.getPropertyName(ConnectionProperties.AUTO_BATCH_DDL_OPERATIONS)))
		{
			return this::isOriginalAutoBatchDdlOperations;
		}
		if (propertyName.equalsIgnoreCase(
				ConnectionProperties.getPropertyName(ConnectionProperties.REPORT_DEFAULT_SCHEMA_AS_NULL)))
		{
			return this::isOriginalReportDefaultSchemaAsNull;
		}
		if (propertyName
				.equalsIgnoreCase(ConnectionProperties.getPropertyName(ConnectionProperties.BATCH_READ_ONLY_MODE)))
		{
			return this::isOriginalBatchReadOnly;
		}
		// Return a no-op to avoid null checks
		return () -> false;
	}

	@FunctionalInterface
	static interface SqlFunction<T, R>
	{
		R apply(T t) throws SQLException;
	}

	private SqlFunction<Boolean, Integer> getPropertySetter(String propertyName)
	{
		if (propertyName
				.equalsIgnoreCase(ConnectionProperties.getPropertyName(ConnectionProperties.ALLOW_EXTENDED_MODE)))
		{
			return this::setAllowExtendedMode;
		}
		if (propertyName
				.equalsIgnoreCase(ConnectionProperties.getPropertyName(ConnectionProperties.ASYNC_DDL_OPERATIONS)))
		{
			return this::setAsyncDdlOperations;
		}
		if (propertyName
				.equalsIgnoreCase(ConnectionProperties.getPropertyName(ConnectionProperties.AUTO_BATCH_DDL_OPERATIONS)))
		{
			return this::setAutoBatchDdlOperations;
		}
		if (propertyName.equalsIgnoreCase(
				ConnectionProperties.getPropertyName(ConnectionProperties.REPORT_DEFAULT_SCHEMA_AS_NULL)))
		{
			return this::setReportDefaultSchemaAsNull;
		}
		if (propertyName
				.equalsIgnoreCase(ConnectionProperties.getPropertyName(ConnectionProperties.BATCH_READ_ONLY_MODE)))
		{
			return this::setBatchReadOnly;
		}
		// Return a no-op to avoid null checks
		return x -> 0;
	}

	public ResultSet getDynamicConnectionProperties(CloudSpannerStatement statement)
	{
		return getDynamicConnectionProperty(statement, null);
	}

	public ResultSet getDynamicConnectionProperty(CloudSpannerStatement statement, String propertyName)
	{
		Map<String, String> values = new HashMap<>();
		if (propertyName == null || propertyName
				.equalsIgnoreCase(ConnectionProperties.getPropertyName(ConnectionProperties.ALLOW_EXTENDED_MODE)))
		{
			values.put(ConnectionProperties.getPropertyName(ConnectionProperties.ALLOW_EXTENDED_MODE),
					String.valueOf(isAllowExtendedMode()));
		}
		if (propertyName == null || propertyName
				.equalsIgnoreCase(ConnectionProperties.getPropertyName(ConnectionProperties.ASYNC_DDL_OPERATIONS)))
		{
			values.put(ConnectionProperties.getPropertyName(ConnectionProperties.ASYNC_DDL_OPERATIONS),
					String.valueOf(isAsyncDdlOperations()));
		}
		if (propertyName == null || propertyName
				.equalsIgnoreCase(ConnectionProperties.getPropertyName(ConnectionProperties.AUTO_BATCH_DDL_OPERATIONS)))
		{
			values.put(ConnectionProperties.getPropertyName(ConnectionProperties.AUTO_BATCH_DDL_OPERATIONS),
					String.valueOf(isAutoBatchDdlOperations()));
		}
		if (propertyName == null || propertyName.equalsIgnoreCase(
				ConnectionProperties.getPropertyName(ConnectionProperties.REPORT_DEFAULT_SCHEMA_AS_NULL)))
		{
			values.put(ConnectionProperties.getPropertyName(ConnectionProperties.REPORT_DEFAULT_SCHEMA_AS_NULL),
					String.valueOf(isReportDefaultSchemaAsNull()));
		}
		if (propertyName == null || propertyName
				.equalsIgnoreCase(ConnectionProperties.getPropertyName(ConnectionProperties.BATCH_READ_ONLY_MODE)))
		{
			values.put(ConnectionProperties.getPropertyName(ConnectionProperties.BATCH_READ_ONLY_MODE),
					String.valueOf(isBatchReadOnly()));
		}
		return createResultSet(statement, values);
	}

	private ResultSet createResultSet(CloudSpannerStatement statement, Map<String, String> values)
	{
		List<Struct> rows = new ArrayList<>(values.size());
		for (Entry<String, String> entry : values.entrySet())
		{
			rows.add(Struct.newBuilder().add("NAME", Value.string(entry.getKey()))
					.add("VALUE", Value.string(entry.getValue())).build());
		}
		com.google.cloud.spanner.ResultSet rs = ResultSets.forRows(
				Type.struct(StructField.of("NAME", Type.string()), StructField.of("VALUE", Type.string())), rows);
		return new CloudSpannerResultSet(statement, rs, null);
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
	 * @return The read timestamp for the current read-only transaction, or null
	 *         if there is no read-only transaction
	 */
	@Override
	public Timestamp getReadTimestamp()
	{
		return transaction == null ? null : transaction.getReadTimestamp();
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
		getTransaction().prepareTransaction(xid);
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
		getTransaction().commitPreparedTransaction(xid);
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
		getTransaction().rollbackPreparedTransaction(xid);
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

	@Override
	public boolean isBatchReadOnly()
	{
		return batchReadOnly;
	}

	@Override
	public int setBatchReadOnly(boolean batchReadOnly) throws SQLException
	{
		checkClosed();
		if (batchReadOnly != this.batchReadOnly)
		{
			if (getAutoCommit())
			{
				throw new CloudSpannerSQLException(
						"The connection is currently in auto-commit mode. Please turn off auto-commit before changing batch read-only mode.",
						Code.FAILED_PRECONDITION);
			}
			if (getTransaction().isRunning())
			{
				throw new CloudSpannerSQLException(
						"There is currently a transaction running. Commit or rollback the running transaction before changing batch read-only mode.",
						Code.FAILED_PRECONDITION);
			}
		}
		this.batchReadOnly = batchReadOnly;
		return 1;
	}

	boolean isOriginalBatchReadOnly()
	{
		return originalBatchReadOnly;
	}

	void setOriginalBatchReadOnly(boolean originalBatchReadOnly)
	{
		this.originalBatchReadOnly = originalBatchReadOnly;
	}

	private void checkSavepointPossible() throws SQLException
	{
		checkClosed();
		if (getAutoCommit())
			throw new CloudSpannerSQLException("Savepoints are not supported in autocommit mode",
					Code.FAILED_PRECONDITION);
		if (isReadOnly() || isBatchReadOnly())
			throw new CloudSpannerSQLException("Savepoints are not supported in read-only mode",
					Code.FAILED_PRECONDITION);
	}

	@Override
	public Savepoint setSavepoint() throws SQLException
	{
		checkSavepointPossible();
		CloudSpannerSavepoint savepoint = CloudSpannerSavepoint.generated();
		transaction.setSavepoint(savepoint);
		return savepoint;
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException
	{
		checkSavepointPossible();
		Preconditions.checkNotNull(name);
		CloudSpannerSavepoint savepoint = CloudSpannerSavepoint.named(name);
		transaction.setSavepoint(savepoint);
		return savepoint;
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException
	{
		checkSavepointPossible();
		Preconditions.checkNotNull(savepoint);
		transaction.rollbackSavepoint(savepoint);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException
	{
		checkSavepointPossible();
		Preconditions.checkNotNull(savepoint);
		transaction.releaseSavepoint(savepoint);
	}

}
