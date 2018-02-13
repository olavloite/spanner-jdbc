package nl.topicus.sql2;

import nl.topicus.java.sql2.ConnectionProperty;
import nl.topicus.java.sql2.DataSource;
import nl.topicus.sql2.CloudSpannerConnection.CloudSpannerConnectionBuilder;

public class CloudSpannerDataSource implements DataSource
{
	private ConnectionProperties connectionProperties;

	private CloudSpannerDataSource(ConnectionProperties connectionProperties)
	{
		this.connectionProperties = connectionProperties;
	}

	public static class CloudSpannerDataSourceBuilder implements Builder
	{
		private ConnectionProperties connectionProperties = new ConnectionProperties();

		CloudSpannerDataSourceBuilder()
		{
		}

		@Override
		public CloudSpannerDataSourceBuilder defaultConnectionProperty(ConnectionProperty property, Object value)
		{
			connectionProperties.set(property, null);
			return this;
		}

		@Override
		public CloudSpannerDataSourceBuilder connectionProperty(ConnectionProperty property, Object value)
		{
			connectionProperties.set(property, value);
			return this;
		}

		@Override
		public CloudSpannerDataSourceBuilder registerConnectionProperty(ConnectionProperty property)
		{
			connectionProperties.set(property, null);
			return this;
		}

		@Override
		public CloudSpannerDataSource build()
		{
			return new CloudSpannerDataSource(connectionProperties);
		}

	}

	@Override
	public CloudSpannerConnectionBuilder builder()
	{
		if (connectionProperties == null)
			throw new IllegalStateException("DataSource is closed");
		return new CloudSpannerConnectionBuilder(connectionProperties);
	}

	@Override
	public void close()
	{
		connectionProperties = null;
	}

}
