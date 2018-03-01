package nl.topicus.sql2.operation;

class SingleRowWhereClauseValidatorExpressionVisitorAdapter extends AbstractSpannerExpressionVisitorAdapter
{
	private SingleRowWhereClauseValidator validator;

	SingleRowWhereClauseValidatorExpressionVisitorAdapter(ParameterStore parameterStore,
			SingleRowWhereClauseValidator validator)
	{
		super(parameterStore);
		this.validator = validator;
	}

	@Override
	protected void setValue(Object value)
	{
		validator.to(value);
	}

}
