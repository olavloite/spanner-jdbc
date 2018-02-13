package nl.topicus.sql2.connectionproperty;

public class CloudSpannerInstanceConnectionProperty extends AbstractCloudSpannerSingleValueConnectionProperty<String>
{
	public static final CloudSpannerInstanceConnectionProperty INSTANCE = new CloudSpannerInstanceConnectionProperty();

	private CloudSpannerInstanceConnectionProperty()
	{
		super("instance");
	}

	@Override
	public Class<String> range()
	{
		return String.class;
	}

}
