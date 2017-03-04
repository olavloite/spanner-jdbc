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

	void setParameter(int parameterIndex, Object value)
	{
		highestIndex = Math.max(parameterIndex, highestIndex);
		int arrayIndex = parameterIndex - 1;
		if (arrayIndex >= parameters.length)
		{
			parameters = Arrays.copyOf(parameters, Math.max(parameters.length * 2, arrayIndex));
		}
		parameters[arrayIndex] = value;
	}

}
