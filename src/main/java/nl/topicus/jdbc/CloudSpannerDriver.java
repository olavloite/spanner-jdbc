package nl.topicus.jdbc;

import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.SpannerOptions.Builder;
import com.google.common.base.Preconditions;
import nl.topicus.jdbc.CloudSpannerConnection.CloudSpannerDatabaseSpecification;

public class CloudSpannerDriver implements Driver {
  static {
    try {
      register();
    } catch (SQLException e) {
      java.sql.DriverManager.println("Registering driver failed: " + e.getMessage());
    }
  }
  private static CloudSpannerDriver registeredDriver;

  public static final int DEBUG = 2;
  public static final int INFO = 1;
  public static final int OFF = 0;

  private static final Logger logger = new Logger();
  static boolean logLevelSet = false;
  // the number of milliseconds before a transaction is considered long-running
  private static long longTransactionTrigger = 10000L;

  static final int MAJOR_VERSION = 1;

  static final int MINOR_VERSION = 0;

  static class SpannerKey {
    private final String host;

    private final String projectId;

    private final Credentials credentials;

    private SpannerKey(String host, String projectId, Credentials credentials) {
      this.host = host;
      this.projectId = projectId;
      this.credentials = credentials;
    }

    private static SpannerKey of(String host, String projectId, Credentials credentials) {
      return new SpannerKey(host, projectId, credentials);
    }

    @Override
    public int hashCode() {
      return Objects.hash(host, projectId, credentials);
    }

    @Override
    public boolean equals(Object o) {
      if (o == null)
        return false;
      if (!(o instanceof SpannerKey))
        return false;
      SpannerKey other = (SpannerKey) o;
      return Objects.equals(host, other.host) && Objects.equals(projectId, other.projectId)
          && Objects.equals(credentials, other.credentials);
    }
  }

  /**
   * Keep track of all connections that are opened, so that we know which Spanner instances to
   * close.
   */
  private Map<Spanner, List<CloudSpannerConnection>> connections = new HashMap<>();

  /**
   * Keep track of all spanner instances that are opened by the driver so that these can be reused
   * for new connections to the same project and with the same credentials.
   */
  private Map<SpannerKey, Spanner> spanners = new HashMap<>();

  private class CloseSpannerRunnable implements Runnable {
    @Override
    public void run() {
      try {
        closeSpanner();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  /**
   * Thread that will be run as a shutdown hook on closing the application. This thread will close
   * any Spanner instances opened by the driver that are still open.
   */
  private Thread shutdownThread = null;

  /**
   * 
   * @return The registered {@link CloudSpannerDriver}
   */
  public static CloudSpannerDriver getDriver() {
    return registeredDriver;
  }

  /**
   * Connects to a Google Cloud Spanner database.
   * 
   * @param url Connection URL in the form jdbc:cloudspanner://localhost;Project
   *        =projectId;Instance=instanceId ;Database=databaseName;PvtKeyPath
   *        =path_to_key_file;SimulateProductName=product_name
   * @param info Additional connection properties that will be set on the new connection
   * @return An open {@link CloudSpannerConnection}
   * @throws SQLException if an error occurs while connecting to Google Cloud Spanner
   */
  @Override
  public CloudSpannerConnection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url))
      return null;
    // Parse URL
    ConnectionProperties properties = ConnectionProperties.parse(url);
    // Get connection properties from properties
    properties.setAdditionalConnectionProperties(info);

    CloudSpannerDatabaseSpecification database = new CloudSpannerDatabaseSpecification(
        properties.project, properties.instance, properties.database);
    CloudSpannerConnection connection = new CloudSpannerConnection(this, url, database,
        properties.keyFile, properties.oauthToken, info, properties.useCustomHost);
    connection.setSimulateProductName(properties.productName);
    connection.setSimulateMajorVersion(properties.majorVersion);
    connection.setSimulateMinorVersion(properties.minorVersion);
    connection.setUseServerDML(properties.useServerDML);
    connection.setOriginalUseServerDML(properties.useServerDML);
    connection.setAllowExtendedMode(properties.allowExtendedMode);
    connection.setOriginalAllowExtendedMode(properties.allowExtendedMode);
    connection.setAsyncDdlOperations(properties.asyncDdlOperations);
    connection.setOriginalAsyncDdlOperations(properties.asyncDdlOperations);
    connection.setAutoBatchDdlOperations(properties.autoBatchDdlOperations);
    connection.setOriginalAutoBatchDdlOperations(properties.autoBatchDdlOperations);
    connection.setReportDefaultSchemaAsNull(properties.reportDefaultSchemaAsNull);
    connection.setOriginalReportDefaultSchemaAsNull(properties.reportDefaultSchemaAsNull);
    connection.setBatchReadOnly(properties.batchReadOnlyMode);
    connection.setOriginalBatchReadOnly(properties.batchReadOnlyMode);
    connection.setUseCustomHost(properties.useCustomHost);
    registerConnection(connection);

    return connection;
  }

  /**
   * Closes all connections to Google Cloud Spanner that have been opened by this driver during the
   * lifetime of this application. You should call this method when you want to shutdown your
   * application, as this frees up all connections and sessions to Google Cloud Spanner. Failure to
   * do so, will keep sessions open server side and can eventually lead to resource exhaustion. Any
   * open JDBC connection to Cloud Spanner opened by this driver will also be closed by this method.
   * This method is also called automatically in a shutdown hook when the JVM is stopped orderly.
   */
  public synchronized void closeSpanner() {
    try {
      for (Entry<Spanner, List<CloudSpannerConnection>> entry : connections.entrySet()) {
        List<CloudSpannerConnection> list = entry.getValue();
        for (CloudSpannerConnection con : list) {
          if (!con.isClosed()) {
            con.rollback();
            con.markClosed();
          }
        }
        entry.getKey().close();
      }
      connections.clear();
      spanners.clear();
    } catch (SQLException e) {
      throw SpannerExceptionFactory.newSpannerException(e);
    }
  }

  private synchronized void registerConnection(CloudSpannerConnection connection) {
    if (shutdownThread == null) {
      shutdownThread = new Thread(new CloseSpannerRunnable(), "CloudSpannerDriver shutdown hook");
      Runtime.getRuntime().addShutdownHook(shutdownThread);
    }
    List<CloudSpannerConnection> list = connections.get(connection.getSpanner());
    if (list == null) {
      list = new ArrayList<>();
      connections.put(connection.getSpanner(), list);
    }
    list.add(connection);
  }

  synchronized void closeConnection(CloudSpannerConnection connection) {
    List<CloudSpannerConnection> list = connections.get(connection.getSpanner());
    if (list == null)
      throw new IllegalStateException("Connection is not registered");
    if (!list.remove(connection))
      throw new IllegalStateException("Connection is not registered");
  }

  /**
   * Get a {@link Spanner} instance from the pool or create a new one if needed.
   * 
   * @param projectId The projectId to connect to
   * @param credentials The credentials to use for the connection
   * @param host The host to connect to. Normally this is https://spanner.googleapis.com, but you
   *        could also use a (local) emulator. If null, no host will be set and the default host of
   *        Google Cloud Spanner will be used.
   * @return The {@link Spanner} instance to use
   */
  synchronized Spanner getSpanner(String projectId, Credentials credentials, String host) {
    SpannerKey key = SpannerKey.of(host, projectId, credentials);
    Spanner spanner = spanners.get(key);
    if (spanner == null) {
      spanner = createSpanner(key);
      spanners.put(key, spanner);
    }
    return spanner;
  }

  private Spanner createSpanner(SpannerKey key) {
    Builder builder = SpannerOptions.newBuilder();
    if (key.projectId != null)
      builder.setProjectId(key.projectId);
    if (key.credentials != null)
      builder.setCredentials(key.credentials);
    else if (!hasDefaultCredentials())
      builder.setCredentials(NoCredentials.getInstance());
    if (key.host != null)
      builder.setHost(key.host);
    SpannerOptions options = builder.build();
    return options.getService();
  }

  private boolean hasDefaultCredentials() {
    try {
      return GoogleCredentials.getApplicationDefault() != null;
    } catch (IOException e) {
      // ignore
    }
    return false;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith("jdbc:cloudspanner:");
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    if (!acceptsURL(url))
      return new DriverPropertyInfo[0];
    ConnectionProperties properties = ConnectionProperties.parse(url);
    properties.setAdditionalConnectionProperties(info);

    return properties.getPropertyInfo();
  }

  @Override
  public int getMajorVersion() {
    return getDriverMajorVersion();
  }

  public static int getDriverMajorVersion() {
    return MAJOR_VERSION;
  }

  @Override
  public int getMinorVersion() {
    return getDriverMinorVersion();
  }

  public static int getDriverMinorVersion() {
    return MINOR_VERSION;
  }

  public static String getVersion() {
    return "Google Cloud Spanner Driver " + getDriverMajorVersion() + "." + getDriverMinorVersion();
  }

  @Override
  public boolean jdbcCompliant() {
    return true;
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("java.util.logging is not used");
  }

  public static String quoteIdentifier(String identifier) {
    if (identifier == null)
      return identifier;
    if (identifier.charAt(0) == '`' && identifier.charAt(identifier.length() - 1) == '`')
      return identifier;
    return new StringBuilder(identifier.length() + 2).append("`").append(identifier).append("`")
        .toString();
  }

  public static String unquoteIdentifier(String identifier) {
    String res = identifier;
    if (identifier == null)
      return identifier;
    if (identifier.charAt(0) == '`' && identifier.charAt(identifier.length() - 1) == '`')
      res = identifier.substring(1, identifier.length() - 1);
    return res;
  }

  public static long getLongTransactionTrigger() {
    return longTransactionTrigger;
  }

  /**
   * Sets the number of milliseconds that should be used to consider a transaction long-running. If
   * a log writer has been set for JDBC by calling
   * {@link DriverManager#setLogWriter(java.io.PrintWriter)} and the log level of the Cloud Spanner
   * Driver has been set to at least INFO, transactions that are running for more than this number
   * of milliseconds will log this to the log writer. If the log level of the Cloud Spanner Driver
   * is set to at least DEBUG, then the driver will also log the stack trace of the call that
   * started the transaction, making it easier to find the part of your code that is responsible for
   * the long-running transaction.
   * 
   * @param trigger The number of milliseconds that is to be considered a long-running transaction.
   *        Only values larger than zero are allowed.
   */
  public static void setLongTransactionTrigger(long trigger) {
    synchronized (CloudSpannerDriver.class) {
      Preconditions.checkArgument(trigger > 0L);
      longTransactionTrigger = trigger;
    }
  }

  public static void setLogLevel(int logLevel) {
    synchronized (CloudSpannerDriver.class) {
      logger.setLogLevel(logLevel);
      logLevelSet = true;
    }
  }

  public static int getLogLevel() {
    synchronized (CloudSpannerDriver.class) {
      return logger.getLogLevel();
    }
  }

  /**
   * Register the driver against {@link DriverManager}. This is done automatically when the class is
   * loaded. Dropping the driver from DriverManager's list is possible using {@link #deregister()}
   * method.
   *
   * @throws IllegalStateException if the driver is already registered
   * @throws SQLException if registering the driver fails
   */
  public static void register() throws SQLException {
    if (isRegistered()) {
      throw new IllegalStateException(
          "Driver is already registered. It can only be registered once.");
    }
    CloudSpannerDriver registeredDriver = new CloudSpannerDriver();
    DriverManager.registerDriver(registeredDriver);
    CloudSpannerDriver.registeredDriver = registeredDriver;
  }

  /**
   * According to JDBC specification, this driver is registered against {@link DriverManager} when
   * the class is loaded. To avoid leaks, this method allow unregistering the driver so that the
   * class can be gc'ed if necessary.
   *
   * @throws IllegalStateException if the driver is not registered
   * @throws SQLException if deregistering the driver fails
   */
  public static void deregister() throws SQLException {
    if (!isRegistered()) {
      throw new IllegalStateException(
          "Driver is not registered (or it has not been registered using Driver.register() method)");
    }
    registeredDriver.closeSpanner();
    DriverManager.deregisterDriver(registeredDriver);
    registeredDriver = null;
  }

  /**
   * @return {@code true} if the driver is registered against {@link DriverManager}
   */
  public static boolean isRegistered() {
    return registeredDriver != null;
  }

}
