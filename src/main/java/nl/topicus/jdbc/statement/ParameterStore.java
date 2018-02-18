package nl.topicus.jdbc.statement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * 
 * @author loite
 *
 */
public class ParameterStore
{
	private Object[] parameters = new Object[10];

	private Integer[] types = new Integer[10];

	private Integer[] nullable = new Integer[10];

	private Integer[] scalesOrLengths = new Integer[10];

	private String table;

	private String[] columns = new String[10];

	private int highestIndex = 0;

	void clearParameters()
	{
		highestIndex = 0;
		parameters = new Object[10];
		types = new Integer[10];
		nullable = new Integer[10];
		scalesOrLengths = new Integer[10];
		columns = new String[10];
		table = null;
	}

	Object getParameter(int parameterIndex)
	{
		int arrayIndex = parameterIndex - 1;
		if (arrayIndex >= parameters.length)
			return null;
		return parameters[arrayIndex];
	}

	Integer getType(int parameterIndex)
	{
		int arrayIndex = parameterIndex - 1;
		if (arrayIndex >= types.length)
			return null;
		return types[arrayIndex];
	}

	Integer getNullable(int parameterIndex)
	{
		int arrayIndex = parameterIndex - 1;
		if (arrayIndex >= nullable.length)
			return null;
		return nullable[arrayIndex];
	}

	Integer getScaleOrLength(int parameterIndex)
	{
		int arrayIndex = parameterIndex - 1;
		if (arrayIndex >= scalesOrLengths.length)
			return null;
		return scalesOrLengths[arrayIndex];
	}

	String getColumn(int parameterIndex)
	{
		int arrayIndex = parameterIndex - 1;
		if (arrayIndex >= columns.length)
			return null;
		return columns[arrayIndex];
	}

	String getTable()
	{
		return table;
	}

	void setTable(String table)
	{
		this.table = table;
	}

	void setColumn(int parameterIndex, String column)
	{
		setParameter(parameterIndex, getParameter(parameterIndex), getType(parameterIndex),
				getScaleOrLength(parameterIndex), column);
	}

	void setType(int parameterIndex, Integer type)
	{
		setParameter(parameterIndex, getParameter(parameterIndex), type, getScaleOrLength(parameterIndex),
				getColumn(parameterIndex));
	}

	void setParameter(int parameterIndex, Object value)
	{
		setParameter(parameterIndex, value, null, null);
	}

	void setParameter(int parameterIndex, Object value, Integer sqlType, Integer scaleOrLength)
	{
		setParameter(parameterIndex, value, sqlType, scaleOrLength, null);
	}

	void setParameter(int parameterIndex, Object value, Integer sqlType, Integer scaleOrLength, String column)
	{
		highestIndex = Math.max(parameterIndex, highestIndex);
		int arrayIndex = parameterIndex - 1;
		if (arrayIndex >= parameters.length)
		{
			parameters = Arrays.copyOf(parameters, Math.max(parameters.length * 2, arrayIndex));
			types = Arrays.copyOf(types, Math.max(types.length * 2, arrayIndex));
			nullable = Arrays.copyOf(nullable, Math.max(nullable.length * 2, arrayIndex));
			scalesOrLengths = Arrays.copyOf(scalesOrLengths, Math.max(scalesOrLengths.length * 2, arrayIndex));
			columns = Arrays.copyOf(columns, Math.max(columns.length * 2, arrayIndex));
		}
		parameters[arrayIndex] = value;
		types[arrayIndex] = sqlType;
		scalesOrLengths[arrayIndex] = scaleOrLength;
		columns[arrayIndex] = column;
	}

	int getHighestIndex()
	{
		return highestIndex;
	}

	void fetchMetaData(Connection connection) throws SQLException
	{
		if (table != null && !"".equals(table))
		{
			try (ResultSet rsCols = connection.getMetaData().getColumns(null, null, table, null))
			{
				while (rsCols.next())
				{
					String col = rsCols.getString("COLUMN_NAME");
					int arrayIndex = getParameterArrayIndex(col);
					if (arrayIndex > -1)
					{
						scalesOrLengths[arrayIndex] = rsCols.getInt("COLUMN_SIZE");
						types[arrayIndex] = rsCols.getInt("DATA_TYPE");
						nullable[arrayIndex] = rsCols.getInt("NULLABLE");
					}
				}
			}
		}
	}

	private int getParameterArrayIndex(String columnName)
	{
		for (int param = 0; param < highestIndex; param++)
		{
			if (columnName != null && columns[param] != null && columnName.equalsIgnoreCase(columns[param]))
			{
				return param;
			}
		}
		return -1;
	}

}
