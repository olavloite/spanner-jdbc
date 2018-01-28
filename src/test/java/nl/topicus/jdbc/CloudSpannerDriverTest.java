package nl.topicus.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import nl.topicus.jdbc.test.category.UnitTest;

@RunWith(Enclosed.class)
@Category(UnitTest.class)
public class CloudSpannerDriverTest
{
	private static Driver getDriver() throws SQLException
	{
		return DriverManager.getDriver("jdbc:cloudspanner://localhost");
	}

	public static class AcceptsURLTest
	{

		@Test
		public void acceptsCloudSpannerURL() throws SQLException
		{
			Driver driver = getDriver();
			assertTrue(driver.acceptsURL(
					"jdbc:cloudspanner://localhost;Project=adroit-hall-xxx;Instance=test-instance;Database=testdb;PvtKeyPath=C:\\Users\\MyUserName\\Documents\\CloudSpannerKeys\\cloudspanner3.json"));
		}

		@Test
		public void notAcceptsPostgreSQLURL() throws SQLException
		{
			Driver driver = getDriver();
			assertEquals(false, driver.acceptsURL("jdbc:postgresql://localhost:5432/test"));
		}
	}

	public static class ParseURLTest
	{

		@Test
		public void parseURLWithAllParts() throws Exception
		{
			ConnectionProperties properties = ConnectionProperties.parse(
					"jdbc:cloudspanner://localhost;Project=adroit-hall-xxx;Instance=test-instance;Database=testdb;PvtKeyPath=C:\\Users\\MyUserName\\Documents\\CloudSpannerKeys\\cloudspanner3.json;SimulateProductName=PostgreSQL");
			assertEquals("adroit-hall-xxx", properties.project);
			assertEquals("test-instance", properties.instance);
			assertEquals("testdb", properties.database);
			assertEquals("C:\\Users\\MyUserName\\Documents\\CloudSpannerKeys\\cloudspanner3.json", properties.keyFile);
			assertEquals("PostgreSQL", properties.productName);
			assertNull(properties.oauthToken);
		}

		@Test
		public void parseURLAndProperties() throws Exception
		{
			ConnectionProperties properties = ConnectionProperties.parse(
					"jdbc:cloudspanner://localhost;Project=adroit-hall-xxx;Instance=test-instance;Database=testdb;PvtKeyPath=C:\\Users\\MyUserName\\Documents\\CloudSpannerKeys\\cloudspanner3.json;SimulateProductName=PostgreSQL");
			properties.setAdditionalConnectionProperties(createProperties());
			assertProperties(properties);
		}

		@Test
		public void parseOnlyProperties() throws Exception
		{
			ConnectionProperties properties = ConnectionProperties.parse("jdbc:cloudspanner://localhost");
			properties.setAdditionalConnectionProperties(createProperties());
			assertProperties(properties);
		}

		private void assertProperties(ConnectionProperties properties)
		{
			assertEquals("foo", properties.project);
			assertEquals("bar", properties.instance);
			assertEquals("gamma", properties.database);
			assertEquals("C:\\Users\\MyUserName\\Documents\\CloudSpannerKeys\\cloudspanner4.json", properties.keyFile);
			assertEquals("SQL Server", properties.productName);
			assertNull(properties.oauthToken);
		}

		private Properties createProperties()
		{
			Properties info = new Properties();
			info.setProperty("Project", "foo");
			info.setProperty("Instance", "bar");
			info.setProperty("Database", "gamma");
			info.setProperty("PvtKeyPath", "C:\\Users\\MyUserName\\Documents\\CloudSpannerKeys\\cloudspanner4.json");
			info.setProperty("SimulateProductName", "SQL Server");

			return info;
		}

		@Test
		public void driverPropertyInfoWithoutValues() throws SQLException
		{
			Driver driver = getDriver();
			DriverPropertyInfo[] properties = driver.getPropertyInfo("jdbc:cloudspanner://localhost", null);
			assertEquals(12, properties.length);
			for (DriverPropertyInfo property : properties)
			{
				if (property.name.equals("AllowExtendedMode") || property.name.equals("AsyncDdlOperations")
						|| property.name.equals("AutoBatchDdlOperations"))
					assertEquals("false", property.value);
				else if (property.name.equals("ReportDefaultSchemaAsNull"))
					assertEquals("true", property.value);
				else
					assertNull(property.value);
			}
		}

		@Test
		public void driverPropertyInfoWithURLValues() throws SQLException
		{
			Driver driver = getDriver();
			DriverPropertyInfo[] properties = driver.getPropertyInfo(
					"jdbc:cloudspanner://localhost;Project=adroit-hall-xxx;Instance=test-instance;Database=testdb;PvtKeyPath=C:\\Users\\MyUserName\\Documents\\CloudSpannerKeys\\cloudspanner3.json;SimulateProductName=PostgreSQL",
					null);
			assertEquals(12, properties.length);
			assertEquals("adroit-hall-xxx", properties[0].value);
			assertEquals("test-instance", properties[1].value);
			assertEquals("testdb", properties[2].value);
			assertEquals("C:\\Users\\MyUserName\\Documents\\CloudSpannerKeys\\cloudspanner3.json", properties[3].value);
			assertNull(properties[4].value);
			assertEquals("PostgreSQL", properties[5].value);
		}
	}

}
