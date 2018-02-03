package nl.topicus.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerDataSourceTest
{

	@Test
	public void createDataSourceTest() throws SQLException
	{
		CloudSpannerDataSource subject = new CloudSpannerDataSource();
		setCommonDataSourceTestProperties(subject);
		Connection con = subject.getConnection();
		Assert.assertNotNull(con);
		Assert.assertTrue(con.isWrapperFor(CloudSpannerConnection.class));
		CloudSpannerConnection connection = con.unwrap(CloudSpannerConnection.class);
		testCommonDataSourceTestProperties(connection);
	}

	public static void setCommonDataSourceTestProperties(CloudSpannerDataSource subject) throws SQLException
	{
		subject.setProjectId("helpful-adroit-123456");
		subject.setInstanceId("test-instance");
		subject.setDatabase("test");
		subject.setPvtKeyPath("C:\\Users\\MyUserName\\Documents\\CloudSpannerKeys\\cloudspanner-key.json");
		subject.setPvtKeyPath(null);
		subject.setOauthAccessToken("TEST");
		subject.setSimulateProductName("PostgreSQL");
		subject.setAllowExtendedMode(true);
		subject.setLoginTimeout(10);
		subject.setLogWriter(new PrintWriter(System.out));
	}

	public static void testCommonDataSourceTestProperties(CloudSpannerConnection connection)
	{
		Assert.assertEquals("jdbc:cloudspanner://localhost", connection.getUrl());
		Assert.assertEquals("PostgreSQL", connection.getProductName());
		Assert.assertEquals("helpful-adroit-123456", connection.getSuppliedProperties().getProperty("Project"));
		Assert.assertEquals("test-instance", connection.getSuppliedProperties().getProperty("Instance"));
		Assert.assertEquals("test", connection.getSuppliedProperties().getProperty("Database"));
		Assert.assertEquals("TEST", connection.getSuppliedProperties().getProperty("OAuthAccessToken"));
		Assert.assertTrue(connection.isAllowExtendedMode());
	}

}
