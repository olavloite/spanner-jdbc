package nl.topicus.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class CloudSpannerArray implements Array
{
	private static final String FREE_EXCEPTION = "free() has been called, array is no longer available";

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

	public static CloudSpannerArray createArray(CloudSpannerDataType type, List<? extends Object> elements)
	{
		return new CloudSpannerArray(type, elements);
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

	private CloudSpannerArray(CloudSpannerDataType type, List<? extends Object> elements)
	{
		this.type = type;
		this.data = java.lang.reflect.Array.newInstance(type.getJavaClass(), elements.size());
		this.data = elements.toArray((Object[]) data);
	}

	private void checkFree() throws SQLException
	{
		if (data == null)
		{
			throw new SQLException(FREE_EXCEPTION);
		}
	}

	@Override
	public String getBaseTypeName() throws SQLException
	{
		checkFree();
		return type.getTypeName();
	}

	@Override
	public int getBaseType() throws SQLException
	{
		checkFree();
		return type.getSqlType();
	}

	@Override
	public Object getArray() throws SQLException
	{
		checkFree();
		return data;
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException
	{
		checkFree();
		return data;
	}

	@Override
	public Object getArray(long index, int count) throws SQLException
	{
		checkFree();
		return getArray(index, count, null);
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException
	{
		checkFree();
		Object res = java.lang.reflect.Array.newInstance(type.getJavaClass(), count);
		System.arraycopy(data, (int) index - 1, res, 0, count);

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
		this.data = null;
	}

	@Override
	public String toString()
	{
		StringJoiner joiner = new StringJoiner(",", "{", "}");
		if (data != null)
		{
			for (Object o : (Object[]) data)
			{
				joiner.add(o.toString());
			}
		}
		return joiner.toString();
	}

	@Override
	public boolean equals(Object other)
	{
		if (!(other instanceof CloudSpannerArray))
			return false;
		CloudSpannerArray array = (CloudSpannerArray) other;
		return this.type == array.type && Arrays.deepEquals((Object[]) this.data, (Object[]) array.data);
	}

	@Override
	public int hashCode()
	{
		return this.type.hashCode() ^ Arrays.deepHashCode((Object[]) data);
	}

}
