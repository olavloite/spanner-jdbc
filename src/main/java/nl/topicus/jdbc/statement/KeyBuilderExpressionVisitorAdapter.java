package nl.topicus.jdbc.statement;

import com.google.cloud.spanner.Key;

class KeyBuilderExpressionVisitorAdapter extends AbstractSpannerExpressionVisitorAdapter
{
	private Key.Builder keyBuilder;

	KeyBuilderExpressionVisitorAdapter(ParameterStore parameterStore, Key.Builder keyBuilder)
	{
		super(parameterStore);
		this.keyBuilder = keyBuilder;
	}

	@Override
	protected void setValue(Object value)
	{
		keyBuilder.appendObject(value);
	}

}
