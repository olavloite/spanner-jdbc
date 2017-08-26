package nl.topicus.jdbc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;

import nl.topicus.jdbc.CloudSpannerDriver.ConnectionProperties;

public class MockCloudSpannerConnection
{

	public static CloudSpannerConnection create(String url) throws SQLException
	{
		ConnectionProperties properties = ConnectionProperties.parse(url);
		CloudSpannerConnection connection = mock(CloudSpannerConnection.class);
		when(connection.getUrl()).thenReturn(url);
		when(connection.getProductName()).thenReturn(properties.productName);
		return connection;
	}

}
