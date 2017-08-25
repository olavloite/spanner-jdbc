package nl.topicus.jdbc;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.google.cloud.spanner.Spanner;

public class CloudSpannerDriver implements Driver
{
	static
	{
		try
		{
			java.sql.DriverManager.registerDriver(new CloudSpannerDriver());
		}
		catch (SQLException e)
		{
			java.sql.DriverManager.println("Registering driver failed: " + e.getMessage());
		}
	}
	static final int MAJOR_VERSION = 1;

	static final int MINOR_VERSION = 0;

	private static final String PROJECT_URL_PART = "Project=";

	private static final String INSTANCE_URL_PART = "Instance=";

	private static final String DATABASE_URL_PART = "Database=";

	private static final String KEY_FILE_URL_PART = "PvtKeyPath=";

	private static final String OAUTH_ACCESS_TOKEN_URL_PART = "OAuthAccessToken=";

	private static final String SIMULATE_PRODUCT_NAME = "SimulateProductName=";

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
		checkAndSetLogging();

		String[] parts = url.split(":", 3);
		String[] connectionParts = parts[2].split(";");
		String project = null;
		String instance = null;
		String database = null;
		String keyFile = null;
		String oauthToken = null;
		String productName = null;

		// Get connection properties from connection string
		for (int i = 1; i < connectionParts.length; i++)
		{
			String conPart = connectionParts[i].replace(" ", "");
			if (conPart.startsWith(PROJECT_URL_PART))
				project = conPart.substring(PROJECT_URL_PART.length());
			else if (conPart.startsWith(INSTANCE_URL_PART))
				instance = conPart.substring(INSTANCE_URL_PART.length());
			else if (conPart.startsWith(DATABASE_URL_PART))
				database = conPart.substring(DATABASE_URL_PART.length());
			else if (conPart.startsWith(KEY_FILE_URL_PART))
				keyFile = conPart.substring(KEY_FILE_URL_PART.length());
			else if (conPart.startsWith(OAUTH_ACCESS_TOKEN_URL_PART))
				oauthToken = conPart.substring(OAUTH_ACCESS_TOKEN_URL_PART.length());
			else if (conPart.startsWith(SIMULATE_PRODUCT_NAME))
				productName = conPart.substring(SIMULATE_PRODUCT_NAME.length());
			else
				throw new SQLException("Unknown URL parameter " + conPart);
		}
		// Get connection properties from properties
		project = info.getProperty(PROJECT_URL_PART.substring(0, PROJECT_URL_PART.length() - 1), project);
		instance = info.getProperty(INSTANCE_URL_PART.substring(0, INSTANCE_URL_PART.length() - 1), instance);
		database = info.getProperty(DATABASE_URL_PART.substring(0, DATABASE_URL_PART.length() - 1), database);
		keyFile = info.getProperty(KEY_FILE_URL_PART.substring(0, KEY_FILE_URL_PART.length() - 1), keyFile);
		oauthToken = info.getProperty(
				OAUTH_ACCESS_TOKEN_URL_PART.substring(0, OAUTH_ACCESS_TOKEN_URL_PART.length() - 1), oauthToken);
		productName = info.getProperty(SIMULATE_PRODUCT_NAME.substring(0, SIMULATE_PRODUCT_NAME.length() - 1),
				productName);

		CloudSpannerConnection connection = new CloudSpannerConnection(this, url, project, instance, database, keyFile,
				oauthToken);
		connection.setSimulateProductName(productName);
		registerConnection(connection);

		return connection;
	}

	/**
	 * Checks whether a logging properties file has been specified for this VM.
	 * If not, the default logging level is changed to avoid a lot of debugging
	 * logging.
	 */
	private void checkAndSetLogging()
	{
		try
		{
			RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
			List<String> arguments = runtimeMxBean.getInputArguments();
			for (String arg : arguments)
			{
				if (arg.startsWith("-Djava.util.logging.config"))
					return;
			}
		}
		catch (Exception e)
		{
			// ignore
			return;
		}
		Logger logger = LogManager.getLogManager().getLogger("");
		for (Handler handler : logger.getHandlers())
		{
			handler.setLevel(Level.WARNING);
		}
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
		DriverPropertyInfo[] res = new DriverPropertyInfo[0];

		return res;
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

	@Override
	public boolean jdbcCompliant()
	{
		return true;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException("java.util.logging is not used");
	}

}
