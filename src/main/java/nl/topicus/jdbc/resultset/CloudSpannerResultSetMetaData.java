package nl.topicus.jdbc.resultset;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;

public class CloudSpannerResultSetMetaData implements ResultSetMetaData
{
	private ResultSet resultSet;

	public CloudSpannerResultSetMetaData(ResultSet resultSet)
	{
		this.resultSet = resultSet;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		return false;
	}

	@Override
	public int getColumnCount() throws SQLException
	{
		return resultSet.getColumnCount();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException
	{
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException
	{
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException
	{
		return columnNullableUnknown;
	}

	@Override
	public boolean isSigned(int column) throws SQLException
	{
		int type = getColumnType(column);
		return type == Types.DOUBLE || type == Types.BIGINT;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException
	{
		return 0;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException
	{
		return resultSet.getType().getStructFields().get(column - 1).getName();
	}

	@Override
	public String getColumnName(int column) throws SQLException
	{
		return resultSet.getType().getStructFields().get(column - 1).getName();
	}

	@Override
	public String getSchemaName(int column) throws SQLException
	{
		return "";
	}

	@Override
	public int getPrecision(int column) throws SQLException
	{
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException
	{
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException
	{
		return "";
	}

	@Override
	public String getCatalogName(int column) throws SQLException
	{
		return "";
	}

	@Override
	public int getColumnType(int column) throws SQLException
	{
		return extractColumnType(resultSet.getColumnType(column - 1));
	}

	public static int extractColumnType(Type type)
	{
		if (type.equals(Type.bool()))
			return Types.BOOLEAN;
		if (type.equals(Type.bytes()))
			return Types.BINARY;
		if (type.equals(Type.date()))
			return Types.DATE;
		if (type.equals(Type.float64()))
			return Types.DOUBLE;
		if (type.equals(Type.int64()))
			return Types.BIGINT;
		if (type.equals(Type.string()))
			return Types.VARCHAR;
		if (type.equals(Type.timestamp()))
			return Types.TIMESTAMP;
		return Types.OTHER;
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException
	{
		return resultSet.getColumnType(column - 1).getCode().name();
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException
	{
		return true;
	}

	@Override
	public boolean isWritable(int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException
	{
		return false;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

}
