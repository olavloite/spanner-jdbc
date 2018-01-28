package nl.topicus.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.cloud.spanner.Spanner;

import nl.topicus.jdbc.CloudSpannerConnection.CloudSpannerDatabaseSpecification;

public class CloudSpannerDriver implements Driver
{
	static
	{
		try
		{
			register();
		}
		catch (SQLException e)
		{
			java.sql.DriverManager.println("Registering driver failed: " + e.getMessage());
		}
	}
	private static CloudSpannerDriver registeredDriver;

	public static final int DEBUG = 2;
	public static final int INFO = 1;
	public static final int OFF = 0;

	private static final Logger logger = new Logger();
	static boolean logLevelSet = false;

	static final int MAJOR_VERSION = 1;

	static final int MINOR_VERSION = 0;

	/**
	 * Keep track of all connections that are opened, so that we know which
	 * Spanner instances to close.
	 */
	private Map<Spanner, List<CloudSpannerConnection>> connections = new HashMap<>();

	/**
	 * Connects to a Google Cloud Spanner database.
	 * 
	 * @param url
	 *            Connection URL in the form
	 *            jdbc:cloudspanner://localhost;Project
	 *            =projectId;Instance=instanceId
	 *            ;Database=databaseName;PvtKeyPath
	 *            =path_to_key_file;SimulateProductName=product_name
	 * @param info
	 *            not used
	 * @return A CloudSpannerConnection
	 * @throws SQLException
	 *             if an error occurs while connecting to Google Cloud Spanner
	 */
	@Override
	public Connection connect(String url, Properties info) throws SQLException
	{
		if (!acceptsURL(url))
			return null;
		// Parse URL
		ConnectionProperties properties = ConnectionProperties.parse(url);
		// Get connection properties from properties
		properties.setAdditionalConnectionProperties(info);

		CloudSpannerDatabaseSpecification database = new CloudSpannerDatabaseSpecification(properties.project,
				properties.instance, properties.database);
		CloudSpannerConnection connection = new CloudSpannerConnection(this, url, database, properties.keyFile,
				properties.oauthToken, info);
		connection.setSimulateProductName(properties.productName);
		connection.setSimulateMajorVersion(properties.majorVersion);
		connection.setSimulateMinorVersion(properties.minorVersion);
		connection.setAllowExtendedMode(properties.allowExtendedMode);
		connection.setOriginalAllowExtendedMode(properties.allowExtendedMode);
		connection.setAsyncDdlOperations(properties.asyncDdlOperations);
		connection.setOriginalAsyncDdlOperations(properties.asyncDdlOperations);
		connection.setAutoBatchDdlOperations(properties.autoBatchDdlOperations);
		connection.setOriginalAutoBatchDdlOperations(properties.autoBatchDdlOperations);
		connection.setReportDefaultSchemaAsNull(properties.reportDefaultSchemaAsNull);
		connection.setOriginalReportDefaultSchemaAsNull(properties.reportDefaultSchemaAsNull);
		registerConnection(connection);

		return connection;
	}

	private void registerConnection(CloudSpannerConnection connection)
	{
		List<CloudSpannerConnection> list = connections.get(connection.getSpanner());
		if (list == null)
		{
			list = new ArrayList<>();
			connections.put(connection.getSpanner(), list);
		}
		list.add(connection);
	}

	void closeConnection(CloudSpannerConnection connection)
	{
		List<CloudSpannerConnection> list = connections.get(connection.getSpanner());
		if (list == null)
			throw new IllegalStateException("Connection is not registered");
		if (!list.remove(connection))
			throw new IllegalStateException("Connection is not registered");

		if (list.isEmpty())
		{
			Spanner spanner = connection.getSpanner();
			connections.remove(spanner);
			spanner.close();
		}
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException
	{
		return url.startsWith("jdbc:cloudspanner:");
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
	{
		if (!acceptsURL(url))
			return new DriverPropertyInfo[0];
		ConnectionProperties properties = ConnectionProperties.parse(url);
		properties.setAdditionalConnectionProperties(info);

		return properties.getPropertyInfo();
	}

	@Override
	public int getMajorVersion()
	{
		return getDriverMajorVersion();
	}

	public static int getDriverMajorVersion()
	{
		return MAJOR_VERSION;
	}

	@Override
	public int getMinorVersion()
	{
		return getDriverMinorVersion();
	}

	public static int getDriverMinorVersion()
	{
		return MINOR_VERSION;
	}

	public static String getVersion()
	{
		return "Google Cloud Spanner Driver " + getDriverMajorVersion() + "." + getDriverMinorVersion();
	}

	@Override
	public boolean jdbcCompliant()
	{
		return true;
	}

	@Override
	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException("java.util.logging is not used");
	}

	public static String quoteIdentifier(String identifier)
	{
		if (identifier == null)
			return identifier;
		if (identifier.charAt(0) == '`' && identifier.charAt(identifier.length() - 1) == '`')
			return identifier;
		return new StringBuilder(identifier.length() + 2).append("`").append(identifier).append("`").toString();
	}

	public static String unquoteIdentifier(String identifier)
	{
		String res = identifier;
		if (identifier == null)
			return identifier;
		if (identifier.charAt(0) == '`' && identifier.charAt(identifier.length() - 1) == '`')
			res = identifier.substring(1, identifier.length() - 1);
		return res;
	}

	public static void setLogLevel(int logLevel)
	{
		synchronized (CloudSpannerDriver.class)
		{
			logger.setLogLevel(logLevel);
			logLevelSet = true;
		}
	}

	public static int getLogLevel()
	{
		synchronized (CloudSpannerDriver.class)
		{
			return logger.getLogLevel();
		}
	}

	/**
	 * Register the driver against {@link DriverManager}. This is done
	 * automatically when the class is loaded. Dropping the driver from
	 * DriverManager's list is possible using {@link #deregister()} method.
	 *
	 * @throws IllegalStateException
	 *             if the driver is already registered
	 * @throws SQLException
	 *             if registering the driver fails
	 */
	public static void register() throws SQLException
	{
		if (isRegistered())
		{
			throw new IllegalStateException("Driver is already registered. It can only be registered once.");
		}
		CloudSpannerDriver registeredDriver = new CloudSpannerDriver();
		DriverManager.registerDriver(registeredDriver);
		CloudSpannerDriver.registeredDriver = registeredDriver;
	}

	/**
	 * According to JDBC specification, this driver is registered against
	 * {@link DriverManager} when the class is loaded. To avoid leaks, this
	 * method allow unregistering the driver so that the class can be gc'ed if
	 * necessary.
	 *
	 * @throws IllegalStateException
	 *             if the driver is not registered
	 * @throws SQLException
	 *             if deregistering the driver fails
	 */
	public static void deregister() throws SQLException
	{
		if (!isRegistered())
		{
			throw new IllegalStateException(
					"Driver is not registered (or it has not been registered using Driver.register() method)");
		}
		DriverManager.deregisterDriver(registeredDriver);
		registeredDriver = null;
	}

	/**
	 * @return {@code true} if the driver is registered against
	 *         {@link DriverManager}
	 */
	public static boolean isRegistered()
	{
		return registeredDriver != null;
	}

}
