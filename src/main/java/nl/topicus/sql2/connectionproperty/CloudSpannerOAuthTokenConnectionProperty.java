package nl.topicus.sql2.connectionproperty;

public class CloudSpannerOAuthTokenConnectionProperty extends AbstractCloudSpannerSingleValueConnectionProperty<String>
{
	public CloudSpannerOAuthTokenConnectionProperty()
	{
		super("oauthtoken");
	}

	@Override
	public Class<String> range()
	{
		return String.class;
	}

}
