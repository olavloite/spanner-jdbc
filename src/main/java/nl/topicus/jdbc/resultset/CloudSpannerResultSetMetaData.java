package nl.topicus.jdbc.resultset;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import com.google.cloud.spanner.ResultSet;

import nl.topicus.jdbc.metadata.AbstractCloudSpannerWrapper;

public class CloudSpannerResultSetMetaData extends AbstractCloudSpannerWrapper implements ResultSetMetaData
{
	private ResultSet resultSet;

	public CloudSpannerResultSetMetaData(ResultSet resultSet)
	{
		this.resultSet = resultSet;
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
		int colType = getColumnType(column);
		switch (colType)
		{
		case Types.ARRAY:
			return 50;
		case Types.BOOLEAN:
			return 5;
		case Types.BINARY:
			return 50;
		case Types.DATE:
			return 10;
		case Types.DOUBLE:
			return 14;
		case Types.BIGINT:
			return 10;
		case Types.NVARCHAR:
			return 50;
		case Types.TIMESTAMP:
			return 16;
		default:
			return 10;
		}
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
		// TODO: implement
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException
	{
		// TODO: implement
		return 0;
	}

	@Override
	public String getTableName(int column) throws SQLException
	{
		// TODO: implement
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

	@Override
	public String getColumnTypeName(int column) throws SQLException
	{
		return resultSet.getColumnType(column - 1).getCode().name();
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException
	{
		// TODO: implement
		return false;
	}

	@Override
	public boolean isWritable(int column) throws SQLException
	{
		// TODO: implement
		return true;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException
	{
		// TODO: implement
		return false;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException
	{
		return getClassName(getColumnType(column));
	}

	@Override
	public String toString()
	{
		StringBuilder res = new StringBuilder();
		try
		{
			for (int col = 1; col <= getColumnCount(); col++)
			{
				res.append("Col ").append(col).append(": ");
				res.append(getColumnName(col)).append(" ").append(getColumnTypeName(col));
				res.append("\n");
			}
		}
		catch (SQLException e)
		{
			return "An error occurred while generating string: " + e.getMessage();
		}
		return res.toString();
	}

}
