package nl.topicus.sql2.connectionproperty;

public class CloudSpannerCredentialsPathConnectionProperty
		extends AbstractCloudSpannerSingleValueConnectionProperty<String>
{
	public static final CloudSpannerCredentialsPathConnectionProperty PVTKEYPATH = new CloudSpannerCredentialsPathConnectionProperty();

	public CloudSpannerCredentialsPathConnectionProperty()
	{
		super("pvtkeypath");
	}

	@Override
	public Class<String> range()
	{
		return String.class;
	}

}
