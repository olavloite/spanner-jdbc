package nl.topicus.jdbc.statement;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.google.cloud.spanner.DatabaseClient;

import nl.topicus.jdbc.CloudSpannerConnection;

/**
 * 
 * @author loite
 *
 */
public abstract class AbstractCloudSpannerPreparedStatement extends CloudSpannerStatement implements PreparedStatement
{
	private ParameterStore parameters = new ParameterStore();

	public AbstractCloudSpannerPreparedStatement(CloudSpannerConnection connection, DatabaseClient dbClient)
	{
		super(connection, dbClient);
	}

	protected ParameterStore getParameterStore()
	{
		return parameters;
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException
	{
		parameters.setParameter(parameterIndex, null, sqlType, null);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
		parameters.setParameter(parameterIndex, x, null, length);
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
		parameters.setParameter(parameterIndex, x, null, length);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
		parameters.setParameter(parameterIndex, x, null, length);
	}

	@Override
	public void clearParameters() throws SQLException
	{
		parameters.clearParameters();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
	{
		parameters.setParameter(parameterIndex, x, targetSqlType, null);
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
	{
		parameters.setParameter(parameterIndex, reader, null, length);
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		try (ResultSet rs = executeQuery())
		{
			return rs.getMetaData();
		}
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
	{
		parameters.setParameter(parameterIndex, null, sqlType, null);
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException
	{
		parameters.setParameter(parameterIndex, value);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
	{
		parameters.setParameter(parameterIndex, value);
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException
	{
		parameters.setParameter(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
	{
		parameters.setParameter(parameterIndex, reader);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
	{
		parameters.setParameter(parameterIndex, inputStream);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
	{
		parameters.setParameter(parameterIndex, reader);
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
	{
		parameters.setParameter(parameterIndex, xmlObject);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException
	{
		parameters.setParameter(parameterIndex, x, targetSqlType, scaleOrLength);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
	{
		parameters.setParameter(parameterIndex, reader);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
	{
		parameters.setParameter(parameterIndex, x);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
	{
		parameters.setParameter(parameterIndex, reader);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
	{
		parameters.setParameter(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException
	{
		parameters.setParameter(parameterIndex, reader);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
	{
		parameters.setParameter(parameterIndex, inputStream);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException
	{
		parameters.setParameter(parameterIndex, reader);
	}

}
