package nl.topicus.jdbc.xa;

import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDataSourceTest;
import nl.topicus.jdbc.CloudSpannerXADataSource;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerXADataSourceTest
{

	@Test
	public void testGetConnection() throws SQLException
	{
		CloudSpannerXADataSource subject = new CloudSpannerXADataSource();
		CloudSpannerDataSourceTest.setCommonDataSourceTestProperties(subject);
		subject.setCreateXATable(false);
		CloudSpannerXAConnection connection = subject.getXAConnection();
		assertNotNull(connection);
		assertNotNull(subject.getDescription());
		Connection con = connection.getConnection();
		Assert.assertTrue(con.isWrapperFor(CloudSpannerConnection.class));
		CloudSpannerConnection cloudSpannerConnection = con.unwrap(CloudSpannerConnection.class);
		CloudSpannerDataSourceTest.testCommonDataSourceTestProperties(cloudSpannerConnection);
	}

	@Test
	public void testGetConnectionWithUserNameAndPassword() throws SQLException
	{
		CloudSpannerXADataSource subject = new CloudSpannerXADataSource();
		CloudSpannerDataSourceTest.setCommonDataSourceTestProperties(subject);
		subject.setCreateXATable(false);
		CloudSpannerXAConnection connection = subject.getXAConnection("test", "test");
		assertNotNull(connection);
		Connection con = connection.getConnection();
		Assert.assertTrue(con.isWrapperFor(CloudSpannerConnection.class));
		CloudSpannerConnection cloudSpannerConnection = con.unwrap(CloudSpannerConnection.class);
		CloudSpannerDataSourceTest.testCommonDataSourceTestProperties(cloudSpannerConnection);
	}

}
