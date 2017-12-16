package nl.topicus.jdbc.statement;

class KeyBuilderExpressionVisitorAdapter extends AbstractSpannerExpressionVisitorAdapter
{
	private DeleteKeyBuilder keyBuilder;

	KeyBuilderExpressionVisitorAdapter(ParameterStore parameterStore, String column, DeleteKeyBuilder keyBuilder)
	{
		super(parameterStore, column);
		this.keyBuilder = keyBuilder;
	}

	@Override
	protected void setValue(Object value)
	{
		keyBuilder.to(value);
	}

}
