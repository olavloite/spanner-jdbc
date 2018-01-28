package nl.topicus.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import nl.topicus.jdbc.metadata.AbstractCloudSpannerWrapper;

/**
 * A basic DataSource implementation for getting a Connection to a Google Cloud
 * Spanner database.
 * 
 * @author loite
 *
 */
public class CloudSpannerDataSource extends AbstractCloudSpannerWrapper implements DataSource
{
	private static final String URL = "jdbc:cloudspanner://localhost";

	private String projectId;

	private String instanceId;

	private String database;

	private String pvtKeyPath;

	private String oauthAccessToken;

	private String simulateProductName;

	private boolean allowExtendedMode;

	private PrintWriter logger;

	private int loginTimeout = 0;

	@Override
	public PrintWriter getLogWriter() throws SQLException
	{
		return logger;
	}

	@Override
	public void setLogWriter(PrintWriter out) throws SQLException
	{
		this.logger = out;
	}

	@Override
	public void setLoginTimeout(int seconds) throws SQLException
	{
		loginTimeout = seconds;
	}

	@Override
	public int getLoginTimeout() throws SQLException
	{
		return loginTimeout;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException("java.util.logging is not used");
	}

	@Override
	public Connection getConnection() throws SQLException
	{
		return DriverManager.getConnection(getURL(), getProperties());
	}

	/**
	 * This method will just call {@link #getConnection()} as a username and
	 * password is not needed for a connection. Instead you need to supply a key
	 * file.
	 */
	@Override
	public Connection getConnection(String username, String password) throws SQLException
	{
		return getConnection();
	}

	private String getURL()
	{
		return URL;
	}

	private Properties getProperties()
	{
		Properties info = new Properties();
		setProperty(info, stripEqualsSign(ConnectionProperties.PROJECT_URL_PART), getProjectId());
		setProperty(info, stripEqualsSign(ConnectionProperties.INSTANCE_URL_PART), getInstanceId());
		setProperty(info, stripEqualsSign(ConnectionProperties.DATABASE_URL_PART), getDatabase());
		setProperty(info, stripEqualsSign(ConnectionProperties.KEY_FILE_URL_PART), getPvtKeyPath());
		setProperty(info, stripEqualsSign(ConnectionProperties.OAUTH_ACCESS_TOKEN_URL_PART),
				getOauthAccessToken());
		setProperty(info, stripEqualsSign(ConnectionProperties.SIMULATE_PRODUCT_NAME),
				getSimulateProductName());
		setProperty(info, stripEqualsSign(ConnectionProperties.ALLOW_EXTENDED_MODE),
				isAllowExtendedMode());

		return info;
	}

	private void setProperty(Properties info, String key, Object value)
	{
		if (value != null)
		{
			info.setProperty(key, String.valueOf(value));
		}
	}

	private String stripEqualsSign(String urlPart)
	{
		return urlPart.substring(0, urlPart.length() - 1);
	}

	public String getProjectId()
	{
		return projectId;
	}

	public void setProjectId(String projectId)
	{
		this.projectId = projectId;
	}

	public String getInstanceId()
	{
		return instanceId;
	}

	public void setInstanceId(String instanceId)
	{
		this.instanceId = instanceId;
	}

	public String getDatabase()
	{
		return database;
	}

	public void setDatabase(String database)
	{
		this.database = database;
	}

	public String getPvtKeyPath()
	{
		return pvtKeyPath;
	}

	public void setPvtKeyPath(String pvtKeyPath)
	{
		this.pvtKeyPath = pvtKeyPath;
	}

	public String getOauthAccessToken()
	{
		return oauthAccessToken;
	}

	public void setOauthAccessToken(String oauthAccessToken)
	{
		this.oauthAccessToken = oauthAccessToken;
	}

	public String getSimulateProductName()
	{
		return simulateProductName;
	}

	public void setSimulateProductName(String simulateProductName)
	{
		this.simulateProductName = simulateProductName;
	}

	public boolean isAllowExtendedMode()
	{
		return allowExtendedMode;
	}

	public void setAllowExtendedMode(boolean allowExtendedMode)
	{
		this.allowExtendedMode = allowExtendedMode;
	}

}
