package nl.topicus.jdbc;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.sql.SQLException;

import org.mockito.internal.stubbing.answers.Returns;

public class MockCloudSpannerConnection
{

	public static CloudSpannerConnection create(String url) throws SQLException
	{
		ConnectionProperties properties = ConnectionProperties.parse(url);
		CloudSpannerConnection connection = mock(CloudSpannerConnection.class);
		when(connection.getSimulateMajorVersion()).then(new Returns(null));
		when(connection.getSimulateMinorVersion()).then(new Returns(null));
		when(connection.getUrl()).thenAnswer(new Returns(url));
		when(connection.getProductName()).thenAnswer(new Returns(properties.productName));
		when(connection.getNodeCount()).thenAnswer(new Returns(1));
		return connection;
	}

	public static AbstractCloudSpannerConnection createAbstractConnection()
	{
		return mock(AbstractCloudSpannerConnection.class,
				withSettings().useConstructor().defaultAnswer(CALLS_REAL_METHODS));
	}

}
