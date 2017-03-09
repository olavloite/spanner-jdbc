package nl.topicus.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

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
		}
	}
	static final int MAJOR_VERSION = 1;

	static final int MINOR_VERSION = 0;

	private static final String PROJECT_URL_PART = "Project=";

	private static final String INSTANCE_URL_PART = "Instance=";

	private static final String DATABASE_URL_PART = "Database=";

	private static final String KEY_FILE_URL_PART = "PvtKeyPath=";

	private static final String SIMULATE_PRODUCT_NAME = "SimulateProductName=";

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

		String[] parts = url.split(":", 3);
		String[] connectionParts = parts[2].split(";");
		// String server = connectionParts[0];
		String project = null;
		String instance = null;
		String database = null;
		String keyFile = null;
		String productName = null;

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
			else if (conPart.startsWith(SIMULATE_PRODUCT_NAME))
				productName = conPart.substring(SIMULATE_PRODUCT_NAME.length());
			else
				throw new SQLException("Unknown URL parameter " + conPart);
		}
		CloudSpannerConnection connection = new CloudSpannerConnection(url, project, instance, database, keyFile);
		connection.setSimulateProductName(productName);
		return connection;
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
		return MAJOR_VERSION;
	}

	@Override
	public int getMinorVersion()
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
