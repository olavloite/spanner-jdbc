package nl.topicus.jdbc.resultset;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;

import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;

import nl.topicus.jdbc.CloudSpannerArray;
import nl.topicus.jdbc.CloudSpannerDataType;
import nl.topicus.jdbc.util.CloudSpannerConversionUtil;

public class CloudSpannerResultSet extends AbstractCloudSpannerResultSet
{
	private com.google.cloud.spanner.ResultSet resultSet;

	private boolean closed = false;

	private boolean wasNull = false;

	private boolean beforeFirst = true;

	private boolean afterLast = false;

	private boolean nextCalledForMetaData = false;

	private boolean nextCalledForMetaDataResult = false;

	private Statement statement;

	public CloudSpannerResultSet(com.google.cloud.spanner.ResultSet resultSet)
	{
		this.resultSet = resultSet;
	}

	@Override
	public boolean wasNull() throws SQLException
	{
		return wasNull;
	}

	private void ensureOpenAndInValidPosition() throws SQLException
	{
		ensureOpen();
		ensureAfterFirst();
		ensureBeforeLast();
	}

	private void ensureAfterFirst() throws SQLException
	{
		if (beforeFirst)
			throw new SQLException("Before first record");
	}

	private void ensureBeforeLast() throws SQLException
	{
		if (afterLast)
			throw new SQLException("After last record");
	}

	private void ensureOpen() throws SQLException
	{
		if (closed)
			throw new SQLException("Resultset is closed");
	}

	@Override
	public boolean next() throws SQLException
	{
		ensureOpen();
		if (!beforeFirst && nextCalledForMetaData)
		{
			nextCalledForMetaData = false;
			return nextCalledForMetaDataResult;
		}
		beforeFirst = false;
		boolean res = resultSet.next();
		afterLast = !res;

		return res;
	}

	@Override
	public void close() throws SQLException
	{
		resultSet.close();
		closed = true;
	}

	@Override
	public String getString(int columnIndex) throws SQLException
	{
		return isNull(columnIndex) ? null : resultSet.getString(columnIndex - 1);
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException
	{
		return isNull(columnIndex) ? false : resultSet.getBoolean(columnIndex - 1);
	}

	@Override
	public long getLong(int columnIndex) throws SQLException
	{
		return isNull(columnIndex) ? 0 : resultSet.getLong(columnIndex - 1);
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException
	{
		return isNull(columnIndex) ? 0 : resultSet.getDouble(columnIndex - 1);
	}

	private BigDecimal toBigDecimal(double d, int scale)
	{
		BigDecimal res = BigDecimal.valueOf(d);
		res.setScale(scale);
		return res;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
	{
		return isNull(columnIndex) ? null : toBigDecimal(resultSet.getDouble(columnIndex - 1), scale);
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException
	{
		return isNull(columnIndex) ? null : resultSet.getBytes(columnIndex - 1).toByteArray();
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException
	{
		return isNull(columnIndex) ? null : CloudSpannerConversionUtil.toSqlDate(resultSet.getDate(columnIndex - 1));
	}

	@SuppressWarnings("deprecation")
	private Time toTime(com.google.cloud.Timestamp ts)
	{
		Timestamp sqlTs = ts.toSqlTimestamp();
		sqlTs.setYear(1970 - 1900);
		sqlTs.setMonth(0);
		sqlTs.setDate(1);
		return new Time(sqlTs.getTime());
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException
	{
		return isNull(columnIndex) ? null : toTime(resultSet.getTimestamp(columnIndex - 1));
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException
	{
		return isNull(columnIndex) ? null : resultSet.getTimestamp(columnIndex - 1).toSqlTimestamp();
	}

	@Override
	public String getString(String columnLabel) throws SQLException
	{
		return isNull(columnLabel) ? null : resultSet.getString(columnLabel);
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException
	{
		return isNull(columnLabel) ? false : resultSet.getBoolean(columnLabel);
	}

	@Override
	public long getLong(String columnLabel) throws SQLException
	{
		return isNull(columnLabel) ? 0 : resultSet.getLong(columnLabel);
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException
	{
		return isNull(columnLabel) ? 0 : resultSet.getDouble(columnLabel);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException
	{
		return isNull(columnLabel) ? null : toBigDecimal(resultSet.getDouble(columnLabel), scale);
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException
	{
		return isNull(columnLabel) ? null : resultSet.getBytes(columnLabel).toByteArray();
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException
	{
		return isNull(columnLabel) ? null : CloudSpannerConversionUtil.toSqlDate(resultSet.getDate(columnLabel));
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException
	{
		return isNull(columnLabel) ? null : toTime(resultSet.getTimestamp(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException
	{
		return isNull(columnLabel) ? null : resultSet.getTimestamp(columnLabel).toSqlTimestamp();
	}

	@Override
	public CloudSpannerResultSetMetaData getMetaData() throws SQLException
	{
		ensureOpen();
		if (beforeFirst)
		{
			nextCalledForMetaDataResult = resultSet.next();
			afterLast = !nextCalledForMetaDataResult;
			beforeFirst = false;
			nextCalledForMetaData = true;
		}
		return new CloudSpannerResultSetMetaData(resultSet);
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException
	{
		ensureOpen();
		try
		{
			return resultSet.getColumnIndex(columnLabel) + 1;
		}
		catch (IllegalArgumentException e)
		{
			throw new SQLException("Column not found: " + columnLabel, e);
		}
	}

	private boolean isNull(int columnIndex) throws SQLException
	{
		ensureOpenAndInValidPosition();
		boolean res = resultSet.isNull(columnIndex - 1);
		wasNull = res;
		return res;
	}

	private boolean isNull(String columnName) throws SQLException
	{
		ensureOpenAndInValidPosition();
		boolean res = resultSet.isNull(columnName);
		wasNull = res;
		return res;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException
	{
		return isNull(columnIndex) ? null : toBigDecimal(resultSet.getDouble(columnIndex - 1), 34);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException
	{
		return isNull(columnLabel) ? null : toBigDecimal(resultSet.getDouble(columnLabel), 34);
	}

	@Override
	public Statement getStatement() throws SQLException
	{
		ensureOpen();
		return statement;
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException
	{
		return isNull(columnIndex) ? null : CloudSpannerConversionUtil.toSqlDate(resultSet.getDate(columnIndex - 1));
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException
	{
		return isNull(columnLabel) ? null : CloudSpannerConversionUtil.toSqlDate(resultSet.getDate(columnLabel));
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException
	{
		return isNull(columnIndex) ? null : toTime(resultSet.getTimestamp(columnIndex - 1));
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException
	{
		return isNull(columnLabel) ? null : toTime(resultSet.getTimestamp(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException
	{
		return isNull(columnIndex) ? null : resultSet.getTimestamp(columnIndex - 1).toSqlTimestamp();
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException
	{
		return isNull(columnLabel) ? null : resultSet.getTimestamp(columnLabel).toSqlTimestamp();
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return closed;
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException
	{
		return isNull(columnIndex) ? 0 : (byte) resultSet.getLong(columnIndex - 1);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException
	{
		return isNull(columnIndex) ? 0 : (short) resultSet.getLong(columnIndex - 1);
	}

	@Override
	public int getInt(int columnIndex) throws SQLException
	{
		return isNull(columnIndex) ? 0 : (int) resultSet.getLong(columnIndex - 1);
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException
	{
		return isNull(columnIndex) ? 0 : (float) resultSet.getDouble(columnIndex - 1);
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException
	{
		return isNull(columnLabel) ? 0 : (byte) resultSet.getLong(columnLabel);
	}

	@Override
	public short getShort(String columnLabel) throws SQLException
	{
		return isNull(columnLabel) ? 0 : (short) resultSet.getLong(columnLabel);
	}

	@Override
	public int getInt(String columnLabel) throws SQLException
	{
		return isNull(columnLabel) ? 0 : (int) resultSet.getLong(columnLabel);
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException
	{
		return isNull(columnLabel) ? 0 : (float) resultSet.getDouble(columnLabel);
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException
	{
		Type type = resultSet.getColumnType(columnLabel);
		return isNull(columnLabel) ? null : getObject(type, columnLabel);
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException
	{
		Type type = resultSet.getColumnType(columnIndex - 1);
		return isNull(columnIndex) ? null : getObject(type, columnIndex);
	}

	private Object getObject(Type type, String columnLabel) throws SQLException
	{
		return getObject(type, resultSet.getColumnIndex(columnLabel) + 1);
	}

	private Object getObject(Type type, int columnIndex) throws SQLException
	{
		if (type == Type.bool())
			return getBoolean(columnIndex);
		if (type == Type.bytes())
			return getBytes(columnIndex);
		if (type == Type.date())
			return getDate(columnIndex);
		if (type == Type.float64())
			return getDouble(columnIndex);
		if (type == Type.int64())
			return getLong(columnIndex);
		if (type == Type.string())
			return getString(columnIndex);
		if (type == Type.timestamp())
			return getTimestamp(columnIndex);
		if (type.getCode() == Code.ARRAY)
			return getArray(columnIndex);
		throw new SQLException("Unknown type: " + type.toString());
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException
	{
		Type type = resultSet.getColumnType(columnLabel);
		if (type.getCode() != Code.ARRAY)
			throw new SQLException("Column with label " + columnLabel + " does not contain an array");
		return getArray(resultSet.getColumnIndex(columnLabel) + 1);
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException
	{
		if (isNull(columnIndex))
			return null;
		Type type = resultSet.getColumnType(columnIndex - 1);
		if (type.getCode() != Code.ARRAY)
			throw new SQLException("Column with index " + columnIndex + " does not contain an array");
		CloudSpannerDataType dataType = CloudSpannerDataType.getType(type.getArrayElementType().getCode());
		List<? extends Object> elements = dataType.getArrayElements(resultSet, columnIndex - 1);

		return CloudSpannerArray.createArray(dataType, elements);
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		ensureOpen();
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		ensureOpen();
	}

	@Override
	public boolean isBeforeFirst() throws SQLException
	{
		ensureOpen();
		return beforeFirst;
	}

	@Override
	public boolean isAfterLast() throws SQLException
	{
		ensureOpen();
		return afterLast;
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException
	{
		String val = getString(columnIndex);
		return val == null ? null : new StringReader(val);
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException
	{
		String val = getString(columnLabel);
		return val == null ? null : new StringReader(val);
	}

	private InputStream getInputStream(String val, Charset charset)
	{
		if (val == null)
			return null;
		byte[] b = val.getBytes(charset);
		return new ByteArrayInputStream(b);
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException
	{
		return getInputStream(getString(columnIndex), StandardCharsets.US_ASCII);
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException
	{
		return getInputStream(getString(columnIndex), StandardCharsets.UTF_16LE);
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException
	{
		byte[] val = getBytes(columnIndex);
		return val == null ? null : new ByteArrayInputStream(val);
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException
	{
		return getInputStream(getString(columnLabel), StandardCharsets.US_ASCII);
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException
	{
		return getInputStream(getString(columnLabel), StandardCharsets.UTF_16LE);
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException
	{
		byte[] val = getBytes(columnLabel);
		return val == null ? null : new ByteArrayInputStream(val);
	}

	@Override
	public String getNString(int columnIndex) throws SQLException
	{
		return getString(columnIndex);
	}

	@Override
	public String getNString(String columnLabel) throws SQLException
	{
		return getString(columnLabel);
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException
	{
		return getCharacterStream(columnIndex);
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException
	{
		return getCharacterStream(columnLabel);
	}

}
