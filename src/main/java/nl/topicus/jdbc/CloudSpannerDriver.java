package nl.topicus.jdbc;

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

	static final class ConnectionProperties
	{
		private static final String PROJECT_URL_PART = "Project=";

		private static final String INSTANCE_URL_PART = "Instance=";

		private static final String DATABASE_URL_PART = "Database=";

		private static final String KEY_FILE_URL_PART = "PvtKeyPath=";

		private static final String OAUTH_ACCESS_TOKEN_URL_PART = "OAuthAccessToken=";

		private static final String SIMULATE_PRODUCT_NAME = "SimulateProductName=";

		private static final String ALLOW_EXTENDED_MODE = "AllowExtendedMode=";

		String project = null;
		String instance = null;
		String database = null;
		String keyFile = null;
		String oauthToken = null;
		String productName = null;
		boolean allowExtendedMode = false;

		static ConnectionProperties parse(String url) throws SQLException
		{
			ConnectionProperties res = new ConnectionProperties();
			if (url != null)
			{
				String[] parts = url.split(":", 3);
				String[] connectionParts = parts[2].split(";");
				// Get connection properties from connection string
				for (int i = 1; i < connectionParts.length; i++)
				{
					String conPart = connectionParts[i].replace(" ", "");
					if (conPart.startsWith(PROJECT_URL_PART))
						res.project = conPart.substring(PROJECT_URL_PART.length());
					else if (conPart.startsWith(INSTANCE_URL_PART))
						res.instance = conPart.substring(INSTANCE_URL_PART.length());
					else if (conPart.startsWith(DATABASE_URL_PART))
						res.database = conPart.substring(DATABASE_URL_PART.length());
					else if (conPart.startsWith(KEY_FILE_URL_PART))
						res.keyFile = conPart.substring(KEY_FILE_URL_PART.length());
					else if (conPart.startsWith(OAUTH_ACCESS_TOKEN_URL_PART))
						res.oauthToken = conPart.substring(OAUTH_ACCESS_TOKEN_URL_PART.length());
					else if (conPart.startsWith(SIMULATE_PRODUCT_NAME))
						res.productName = conPart.substring(SIMULATE_PRODUCT_NAME.length());
					else if (conPart.startsWith(ALLOW_EXTENDED_MODE))
					{
						try
						{
							res.allowExtendedMode = Boolean.valueOf(conPart.substring(ALLOW_EXTENDED_MODE.length()));
						}
						catch (NumberFormatException e)
						{
							throw new SQLException("Invalid value for " + conPart + ": "
									+ conPart.substring(ALLOW_EXTENDED_MODE.length()), e);
						}
					}
					else
						throw new SQLException("Unknown URL parameter " + conPart);
				}
			}
			return res;
		}

		void setAdditionalConnectionProperties(Properties info) throws SQLException
		{
			if (info != null)
			{
				project = info.getProperty(PROJECT_URL_PART.substring(0, PROJECT_URL_PART.length() - 1), project);
				instance = info.getProperty(INSTANCE_URL_PART.substring(0, INSTANCE_URL_PART.length() - 1), instance);
				database = info.getProperty(DATABASE_URL_PART.substring(0, DATABASE_URL_PART.length() - 1), database);
				keyFile = info.getProperty(KEY_FILE_URL_PART.substring(0, KEY_FILE_URL_PART.length() - 1), keyFile);
				oauthToken = info.getProperty(
						OAUTH_ACCESS_TOKEN_URL_PART.substring(0, OAUTH_ACCESS_TOKEN_URL_PART.length() - 1), oauthToken);
				productName = info.getProperty(SIMULATE_PRODUCT_NAME.substring(0, SIMULATE_PRODUCT_NAME.length() - 1),
						productName);
				try
				{
					allowExtendedMode = Boolean.valueOf(
							info.getProperty(ALLOW_EXTENDED_MODE.substring(0, ALLOW_EXTENDED_MODE.length() - 1),
									String.valueOf(allowExtendedMode)));
				}
				catch (NumberFormatException e)
				{
					throw new SQLException(
							"Invalid value for " + ALLOW_EXTENDED_MODE.substring(0, ALLOW_EXTENDED_MODE.length() - 1),
							e);
				}
			}
		}

		DriverPropertyInfo[] getPropertyInfo()
		{
			DriverPropertyInfo[] res = new DriverPropertyInfo[7];
			res[0] = new DriverPropertyInfo(PROJECT_URL_PART.substring(0, PROJECT_URL_PART.length() - 1), project);
			res[0].description = "Google Cloud Project id";
			res[1] = new DriverPropertyInfo(INSTANCE_URL_PART.substring(0, INSTANCE_URL_PART.length() - 1), instance);
			res[1].description = "Google Cloud Spanner Instance id";
			res[2] = new DriverPropertyInfo(DATABASE_URL_PART.substring(0, DATABASE_URL_PART.length() - 1), database);
			res[2].description = "Google Cloud Spanner Database name";
			res[3] = new DriverPropertyInfo(KEY_FILE_URL_PART.substring(0, KEY_FILE_URL_PART.length() - 1), keyFile);
			res[3].description = "Path to json key file to be used for authentication";
			res[4] = new DriverPropertyInfo(
					OAUTH_ACCESS_TOKEN_URL_PART.substring(0, OAUTH_ACCESS_TOKEN_URL_PART.length() - 1), oauthToken);
			res[4].description = "OAuth access token to be used for authentication (optional, only to be used when no key file is specified)";
			res[5] = new DriverPropertyInfo(SIMULATE_PRODUCT_NAME.substring(0, SIMULATE_PRODUCT_NAME.length() - 1),
					productName);
			res[5].description = "Use this property to make the driver return a different database product name than Google Cloud Spanner, for example if you are using a framework like Spring that use this property to determine how to a generate data model for Spring Batch";
			res[6] = new DriverPropertyInfo(ALLOW_EXTENDED_MODE.substring(0, ALLOW_EXTENDED_MODE.length() - 1),
					String.valueOf(allowExtendedMode));
			res[6].description = "Allow the driver to enter 'extended' mode for bulk operations. A value of false (default) indicates that the driver should never enter extended mode. If this property is set to true, the driver will execute all bulk DML-operations in a separate transaction when the number of records affected is greater than what will exceed the limitations of Cloud Spanner.";

			return res;
		}
	}

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

		CloudSpannerConnection connection = new CloudSpannerConnection(this, url, properties.project,
				properties.instance, properties.database, properties.keyFile, properties.oauthToken,
				properties.allowExtendedMode, info);
		connection.setSimulateProductName(properties.productName);
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

}
