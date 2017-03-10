package nl.topicus.jdbc.statement;

import javax.xml.bind.DatatypeConverter;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.schema.Column;

abstract class AbstractSpannerExpressionVisitorAdapter<R> extends ExpressionVisitorAdapter
{
	private ParameterStore parameterStore;

	AbstractSpannerExpressionVisitorAdapter(ParameterStore parameterStore)
	{
		this.parameterStore = parameterStore;
	}

	protected abstract void setValue(Object value);

	@Override
	public void visit(JdbcParameter parameter)
	{
		Object value = parameterStore.getParameter(parameter.getIndex());
		setValue(value);
	}

	@Override
	public void visit(NullValue value)
	{
		setValue(null);
	}

	@Override
	public void visit(DoubleValue value)
	{
		setValue(value.getValue());
	}

	@Override
	public void visit(LongValue value)
	{
		setValue(value.getValue());
	}

	@Override
	public void visit(DateValue value)
	{
		setValue(value.getValue());
	}

	@Override
	public void visit(TimeValue value)
	{
		setValue(value.getValue());
	}

	@Override
	public void visit(TimestampValue value)
	{
		setValue(value.getValue());
	}

	@Override
	public void visit(StringValue value)
	{
		setValue(value.getValue());
	}

	@Override
	public void visit(HexValue value)
	{
		String stringValue = value.getValue().substring(2);
		byte[] byteValue = DatatypeConverter.parseHexBinary(stringValue);
		setValue(byteValue);
	}

	/**
	 * Booleans are not recognized by the parser, but are seen as column names.
	 * 
	 * @param column
	 */
	@Override
	public void visit(Column column)
	{
		String stringValue = column.getColumnName();
		if (stringValue.equalsIgnoreCase("true") || stringValue.equalsIgnoreCase("false"))
		{
			setValue(Boolean.valueOf(stringValue));
		}
	}

}