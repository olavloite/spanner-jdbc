package nl.topicus.sql2;

import nl.topicus.java.sql2.DataSourceFactory;
import nl.topicus.sql2.CloudSpannerDataSource.CloudSpannerDataSourceBuilder;

public class CloudSpannerDataSourceFactory implements DataSourceFactory
{
	public static final CloudSpannerDataSourceFactory INSTANCE = new CloudSpannerDataSourceFactory();

	private CloudSpannerDataSourceFactory()
	{
	}

	@Override
	public CloudSpannerDataSourceBuilder builder()
	{
		return new CloudSpannerDataSourceBuilder();
	}

	@Override
	public String getName()
	{
		return "nl.topicus.sql2.CloudSpannerDataSourceFactory";
	}

}
