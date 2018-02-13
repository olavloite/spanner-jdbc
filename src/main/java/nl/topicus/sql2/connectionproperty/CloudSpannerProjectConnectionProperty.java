package nl.topicus.sql2.connectionproperty;

public class CloudSpannerProjectConnectionProperty extends AbstractCloudSpannerSingleValueConnectionProperty<String>
{
	public static final CloudSpannerProjectConnectionProperty PROJECT = new CloudSpannerProjectConnectionProperty();

	private CloudSpannerProjectConnectionProperty()
	{
		super("project");
	}

	@Override
	public Class<String> range()
	{
		return String.class;
	}

}
