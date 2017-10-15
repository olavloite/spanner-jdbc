package nl.topicus.jdbc;

import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import nl.topicus.jdbc.metadata.AbstractCloudSpannerWrapper;

/**
 * 
 * @author loite
 *
 */
public abstract class AbstractCloudSpannerConnection extends AbstractCloudSpannerWrapper
		implements ICloudSpannerConnection
{
	static final String CONNECTION_CLOSED = "Connection closed";

	protected void checkClosed() throws SQLException
	{
		if (isClosed())
		{
			throw new SQLException(CONNECTION_CLOSED);
		}
	}

	@Override
	public void setCatalog(String catalog) throws SQLException
	{
		checkClosed();
		// silently ignore
	}

	@Override
	public String getCatalog() throws SQLException
	{
		checkClosed();
		return null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		checkClosed();
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		checkClosed();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setHoldability(int holdability) throws SQLException
	{
		checkClosed();
		if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
			throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getHoldability() throws SQLException
	{
		checkClosed();
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public Savepoint setSavepoint() throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob createClob() throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob createBlob() throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob createNClob() throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException
	{
		try
		{
			checkClosed();
		}
		catch (SQLException e)
		{
			throw new SQLClientInfoException(e.getMessage(), Collections.emptyMap());
		}
		// silently ignore
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException
	{
		try
		{
			checkClosed();
		}
		catch (SQLException e)
		{
			throw new SQLClientInfoException(e.getMessage(), Collections.emptyMap());
		}
		// silently ignore
	}

	@Override
	public String getClientInfo(String name) throws SQLException
	{
		checkClosed();
		return null;
	}

	@Override
	public Properties getClientInfo() throws SQLException
	{
		checkClosed();
		return new Properties();
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setSchema(String schema) throws SQLException
	{
		checkClosed();
		// silently ignore
	}

	@Override
	public String getSchema() throws SQLException
	{
		checkClosed();
		return null;
	}

	@Override
	public void abort(Executor executor) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getNetworkTimeout() throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
	}

}
