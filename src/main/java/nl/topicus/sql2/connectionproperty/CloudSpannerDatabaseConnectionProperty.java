package nl.topicus.sql2.connectionproperty;

public class CloudSpannerDatabaseConnectionProperty extends AbstractCloudSpannerSingleValueConnectionProperty<String>
{
	public static final CloudSpannerDatabaseConnectionProperty DATABASE = new CloudSpannerDatabaseConnectionProperty();

	private CloudSpannerDatabaseConnectionProperty()
	{
		super("database");
	}

	@Override
	public Class<String> range()
	{
		return String.class;
	}

}
