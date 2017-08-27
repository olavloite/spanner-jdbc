package nl.topicus.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

public class CloudSpannerArray implements Array
{
	private final CloudSpannerDataType type;

	private Object data;

	static CloudSpannerArray createArray(String typeName, Object[] elements) throws SQLException
	{
		for (CloudSpannerDataType type : CloudSpannerDataType.values())
		{
			if (type.getTypeName().equalsIgnoreCase(typeName))
			{
				return new CloudSpannerArray(type, elements);
			}
		}
		throw new SQLException("Data type " + typeName + " is unknown");
	}

	private CloudSpannerArray(CloudSpannerDataType type, Object[] elements) throws SQLException
	{
		this.type = type;
		this.data = java.lang.reflect.Array.newInstance(type.getJavaClass(), elements.length);
		try
		{
			System.arraycopy(elements, 0, this.data, 0, elements.length);
		}
		catch (Exception e)
		{
			throw new SQLException(
					"Could not copy array elements. Make sure the supplied array only contains elements of class "
							+ type.getJavaClass().getName(),
					e);
		}
	}

	@Override
	public String getBaseTypeName() throws SQLException
	{
		return type.getTypeName();
	}

	@Override
	public int getBaseType() throws SQLException
	{
		return type.getSqlType();
	}

	@Override
	public Object getArray() throws SQLException
	{
		return data;
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException
	{
		return data;
	}

	@Override
	public Object getArray(long index, int count) throws SQLException
	{
		return getArray(index, count, null);
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException
	{
		Object res = java.lang.reflect.Array.newInstance(type.getJavaClass(), count);
		System.arraycopy(data, (int) index, res, 0, count);

		return res;
	}

	private static final String RESULTSET_NOT_SUPPORTED = "Getting a resultset from an array is not supported";

	@Override
	public ResultSet getResultSet() throws SQLException
	{
		throw new SQLFeatureNotSupportedException(RESULTSET_NOT_SUPPORTED);
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException
	{
		throw new SQLFeatureNotSupportedException(RESULTSET_NOT_SUPPORTED);
	}

	@Override
	public ResultSet getResultSet(long index, int count) throws SQLException
	{
		throw new SQLFeatureNotSupportedException(RESULTSET_NOT_SUPPORTED);
	}

	@Override
	public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException
	{
		throw new SQLFeatureNotSupportedException(RESULTSET_NOT_SUPPORTED);
	}

	@Override
	public void free() throws SQLException
	{
		this.data = java.lang.reflect.Array.newInstance(type.getJavaClass(), 10);
	}

}
