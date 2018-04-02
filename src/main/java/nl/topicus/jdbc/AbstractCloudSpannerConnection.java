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
import java.sql.Struct;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.google.rpc.Code;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;
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
			throw new CloudSpannerSQLException(CONNECTION_CLOSED, Code.FAILED_PRECONDITION);
		}
	}

	private <T> T checkClosedAndReturnNull() throws SQLException
	{
		checkClosed();
		return null;
	}

	private String checkClosedAndReturnEmptyString() throws SQLException
	{
		checkClosed();
		return "";
	}

	private <T> T checkClosedAndThrowUnsupportedException() throws SQLException
	{
		checkClosed();
		throw new SQLFeatureNotSupportedException();
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
		return isReportDefaultSchemaAsNull() ? checkClosedAndReturnNull() : checkClosedAndReturnEmptyString();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		return checkClosedAndReturnNull();
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		checkClosed();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
	{
		return checkClosedAndThrowUnsupportedException();
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
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException
	{
		return checkClosedAndThrowUnsupportedException();
	}

	@Override
	public Clob createClob() throws SQLException
	{
		return checkClosedAndThrowUnsupportedException();
	}

	@Override
	public Blob createBlob() throws SQLException
	{
		return checkClosedAndThrowUnsupportedException();
	}

	@Override
	public NClob createNClob() throws SQLException
	{
		return checkClosedAndThrowUnsupportedException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException
	{
		return checkClosedAndThrowUnsupportedException();
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
		return checkClosedAndReturnNull();
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
		return checkClosedAndThrowUnsupportedException();
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
		return isReportDefaultSchemaAsNull() ? checkClosedAndReturnNull() : checkClosedAndReturnEmptyString();
	}

	@Override
	public void abort(Executor executor) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
	{
		checkClosedAndThrowUnsupportedException();
	}

	@Override
	public int getNetworkTimeout() throws SQLException
	{
		return checkClosedAndThrowUnsupportedException();
	}

}
