package nl.topicus.jdbc.statement;

class KeyBuilderExpressionVisitorAdapter extends AbstractSpannerExpressionVisitorAdapter
{
	private DeleteKeyBuilder keyBuilder;

	KeyBuilderExpressionVisitorAdapter(ParameterStore parameterStore, DeleteKeyBuilder keyBuilder)
	{
		super(parameterStore);
		this.keyBuilder = keyBuilder;
	}

	@Override
	protected void setValue(Object value)
	{
		keyBuilder.to(value);
	}

}
