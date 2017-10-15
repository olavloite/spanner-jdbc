package nl.topicus.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerPooledConnectionTest
{
	private CloudSpannerPooledConnection subject;

	public CloudSpannerPooledConnectionTest() throws SQLException
	{
		subject = createConnection();
	}

	private static CloudSpannerPooledConnection createConnection() throws SQLException
	{
		CloudSpannerConnectionPoolDataSource ds = new CloudSpannerConnectionPoolDataSource();
		ds.setProjectId("helpful-adroit-123456");
		ds.setInstanceId("test-instance");
		ds.setDatabase("test");
		ds.setPvtKeyPath("C:\\Users\\MyUserName\\Documents\\CloudSpannerKeys\\cloudspanner-key.json");
		ds.setPvtKeyPath(null);
		ds.setOauthAccessToken("TEST");
		ds.setSimulateProductName("PostgreSQL");
		ds.setAllowExtendedMode(true);
		ds.setLoginTimeout(10);
		ds.setLogWriter(new PrintWriter(System.out));
		return ds.getPooledConnection();
	}

	@Test
	public void testPooledConnection() throws SQLException
	{
		Connection con = subject.getConnection();
		Assert.assertNotNull(con);
		Assert.assertFalse(con.isClosed());

		subject.close();
		Assert.assertTrue(con.isClosed());
	}

}
