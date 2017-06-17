package nl.topicus.jdbc.statement;

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

	private Integer[] scalesOrLengths = new Integer[10];

	private int highestIndex = 0;

	void clearParameters()
	{
		parameters = new Object[10];
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

	Integer getScaleOrLength(int parameterIndex)
	{
		int arrayIndex = parameterIndex - 1;
		if (arrayIndex >= scalesOrLengths.length)
			return null;
		return scalesOrLengths[arrayIndex];
	}

	void setParameter(int parameterIndex, Object value)
	{
		setParameter(parameterIndex, value, null, null);
	}

	void setParameter(int parameterIndex, Object value, Integer sqlType, Integer scaleOrLength)
	{
		highestIndex = Math.max(parameterIndex, highestIndex);
		int arrayIndex = parameterIndex - 1;
		if (arrayIndex >= parameters.length)
		{
			parameters = Arrays.copyOf(parameters, Math.max(parameters.length * 2, arrayIndex));
			types = Arrays.copyOf(types, Math.max(types.length * 2, arrayIndex));
			scalesOrLengths = Arrays.copyOf(scalesOrLengths, Math.max(scalesOrLengths.length * 2, arrayIndex));
		}
		parameters[arrayIndex] = value;
		types[arrayIndex] = sqlType;
		scalesOrLengths[arrayIndex] = scaleOrLength;
	}

	int getHighestIndex()
	{
		return highestIndex;
	}

}
