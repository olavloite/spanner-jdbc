package nl.topicus.jdbc.statement;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import nl.topicus.jdbc.metadata.AbstractCloudSpannerWrapper;

public class CloudSpannerParameterMetaData extends AbstractCloudSpannerWrapper implements ParameterMetaData
{
	private final String sql;

	private final ParameterStore parameters;

	private int parameterCount = -1;

	CloudSpannerParameterMetaData(String sql, ParameterStore parameters)
	{
		this.sql = sql;
		this.parameters = parameters;
	}

	@Override
	public int getParameterCount() throws SQLException
	{
		if (parameterCount == -1)
		{
			parameterCount = 0;
			boolean inString = false;
			StringBuilder res = new StringBuilder(sql);
			int i = 0;
			while (i < res.length())
			{
				char c = res.charAt(i);
				if (c == '\'')
				{
					inString = !inString;
				}
				else if (c == '?' && !inString)
				{
					parameterCount++;
				}
				i++;
			}
		}
		return parameterCount;
	}

	@Override
	public int isNullable(int param) throws SQLException
	{
		return parameterNullableUnknown;
	}

	@Override
	public boolean isSigned(int param) throws SQLException
	{
		int type = getParameterType(param);
		return type == Types.DOUBLE || type == Types.FLOAT || type == Types.BIGINT || type == Types.INTEGER
				|| type == Types.DECIMAL;
	}

	@Override
	public int getPrecision(int param) throws SQLException
	{
		return 0;
	}

	@Override
	public int getScale(int param) throws SQLException
	{
		Integer scale = parameters.getScaleOrLength(param);
		return scale == null ? 0 : scale.intValue();
	}

	@Override
	public int getParameterType(int param) throws SQLException
	{
		Integer type = parameters.getType(param);
		if (type != null)
			return type.intValue();

		Object value = parameters.getParameter(param);
		if (value == null)
		{
			return Types.OTHER;
		}
		else if (Boolean.class.isAssignableFrom(value.getClass()))
		{
			return Types.BOOLEAN;
		}
		else if (Byte.class.isAssignableFrom(value.getClass()))
		{
			return Types.TINYINT;
		}
		else if (Integer.class.isAssignableFrom(value.getClass()))
		{
			return Types.INTEGER;
		}
		else if (Long.class.isAssignableFrom(value.getClass()))
		{
			return Types.BIGINT;
		}
		else if (Float.class.isAssignableFrom(value.getClass()))
		{
			return Types.FLOAT;
		}
		else if (Double.class.isAssignableFrom(value.getClass()))
		{
			return Types.DOUBLE;
		}
		else if (BigDecimal.class.isAssignableFrom(value.getClass()))
		{
			return Types.DECIMAL;
		}
		else if (Date.class.isAssignableFrom(value.getClass()))
		{
			return Types.DATE;
		}
		else if (Timestamp.class.isAssignableFrom(value.getClass()))
		{
			return Types.TIMESTAMP;
		}
		else if (String.class.isAssignableFrom(value.getClass()))
		{
			return Types.VARCHAR;
		}
		else if (byte[].class.isAssignableFrom(value.getClass()))
		{
			return Types.BINARY;
		}
		else
		{
			return Types.OTHER;
		}
	}

	@Override
	public String getParameterTypeName(int param) throws SQLException
	{
		return getGoogleTypeName(getParameterType(param));
	}

	@Override
	public String getParameterClassName(int param) throws SQLException
	{
		Object value = parameters.getParameter(param);
		if (value != null)
			return value.getClass().getName();
		Integer type = parameters.getType(param);
		if (type != null)
			return getClassName(type.intValue());
		return null;
	}

	@Override
	public int getParameterMode(int param) throws SQLException
	{
		return parameterModeIn;
	}

	@Override
	public String toString()
	{
		StringBuilder res = new StringBuilder();
		try
		{
			res.append("CloudSpannerPreparedStatementParameterMetaData, parameter count: ").append(getParameterCount());
			for (int param = 1; param <= getParameterCount(); param++)
			{
				res.append("\nParameter ").append(param).append(":\n\t Class name: ")
						.append(getParameterClassName(param));
				res.append(",\n\t Parameter type name: ").append(getParameterTypeName(param));
				res.append(",\n\t Parameter type: ").append(getParameterType(param));
				res.append(",\n\t Parameter precision: ").append(getPrecision(param));
				res.append(",\n\t Parameter scale: ").append(getScale(param));
				res.append(",\n\t Parameter signed: ").append(isSigned(param));
				res.append(",\n\t Parameter nullable: ").append(isNullable(param));
				res.append(",\n\t Parameter mode: ").append(getParameterMode(param));
			}
		}
		catch (SQLException e)
		{
			res.append("Error while fetching parameter metadata: ").append(e.getMessage());
		}
		return res.toString();
	}

}
