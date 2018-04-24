package nl.topicus.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
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
			String pgUrl = "jdbc:postgresql://localhost:5432/test";
			assertEquals(false, driver.acceptsURL(pgUrl));
			assertNull(driver.connect(pgUrl, new Properties()));
			assertEquals(0, driver.getPropertyInfo(pgUrl, new Properties()).length);
		}
	}

	public static class RegisterTest
	{
		@Test
		public void testRegister() throws SQLException
		{
			boolean exception = false;
			try
			{
				// Should fail as the driver was already registered at class
				// load
				CloudSpannerDriver.register();
			}
			catch (IllegalStateException e)
			{
				// expected
				exception = true;
			}
			assertTrue(exception);

			// Should work
			CloudSpannerDriver.deregister();
			// Should work
			CloudSpannerDriver.register();

			// Should fail
			exception = false;
			try
			{
				// Should fail as the driver was already registered manually
				CloudSpannerDriver.register();
			}
			catch (IllegalStateException e)
			{
				// expected
				exception = true;
			}
			assertTrue(exception);

			// Should work
			CloudSpannerDriver.deregister();
			// Should fail
			exception = false;
			try
			{
				// Should fail as the driver was already deregistered manually
				CloudSpannerDriver.deregister();
			}
			catch (IllegalStateException e)
			{
				// expected
				exception = true;
			}
			assertTrue(exception);
			// Should work
			CloudSpannerDriver.register();
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
			assertEquals(ConnectionProperties.NUMBER_OF_PROPERTIES, properties.length);
			for (DriverPropertyInfo property : properties)
			{
				if (property.name.equals("AllowExtendedMode") || property.name.equals("AsyncDdlOperations")
						|| property.name.equals("AutoBatchDdlOperations") || property.name.equals("BatchReadOnlyMode"))
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
			assertEquals(ConnectionProperties.NUMBER_OF_PROPERTIES, properties.length);
			assertEquals("adroit-hall-xxx", properties[0].value);
			assertEquals("test-instance", properties[1].value);
			assertEquals("testdb", properties[2].value);
			assertEquals("C:\\Users\\MyUserName\\Documents\\CloudSpannerKeys\\cloudspanner3.json", properties[3].value);
			assertNull(properties[4].value);
			assertEquals("PostgreSQL", properties[5].value);
		}
	}

	public static class DriverTest
	{
		@Rule
		public ExpectedException thrown = ExpectedException.none();

		@Test
		public void testGetMinorVersion() throws SQLException
		{
			assertEquals(CloudSpannerDriver.MINOR_VERSION, getDriver().getMinorVersion());
		}

		@Test
		public void testGetMajorVersion() throws SQLException
		{
			assertEquals(CloudSpannerDriver.MAJOR_VERSION, getDriver().getMajorVersion());
		}

		@Test
		public void testJdbcCompliant() throws SQLException
		{
			assertTrue(getDriver().jdbcCompliant());
		}

		@Test
		public void testGetParentLogger() throws SQLException
		{
			thrown.expect(SQLFeatureNotSupportedException.class);
			thrown.expectMessage("java.util.logging is not used");
			getDriver().getParentLogger();
		}

		@Test
		public void testQuoteIdentifier() throws SQLException
		{
			assertEquals("`FOO`", CloudSpannerDriver.quoteIdentifier("FOO"));
			assertEquals("`FOO`", CloudSpannerDriver.quoteIdentifier("`FOO`"));
			assertNull(CloudSpannerDriver.quoteIdentifier(null));
		}

		@Test
		public void testUnquoteIdentifier() throws SQLException
		{
			assertEquals("FOO", CloudSpannerDriver.unquoteIdentifier("FOO"));
			assertEquals("FOO", CloudSpannerDriver.unquoteIdentifier("`FOO`"));
			assertNull(CloudSpannerDriver.unquoteIdentifier(null));
		}
	}

	public static class ConnectAndCloseTest
	{
		@Test
		public void testConnect() throws SQLException, NoSuchFieldException, SecurityException,
				IllegalArgumentException, IllegalAccessException
		{
			Connection connection = DriverManager.getConnection(
					"jdbc:cloudspanner://localhost;Project=adroit-hall-xxx;Instance=test-instance;Database=testdb");
			assertNotNull(connection);
			assertTrue(connection.isWrapperFor(ICloudSpannerConnection.class));
			CloudSpannerDriver driver = CloudSpannerDriver.getDriver();
			assertNotNull(driver);
			Field spannersField = CloudSpannerDriver.class.getDeclaredField("spanners");
			spannersField.setAccessible(true);
			@SuppressWarnings("rawtypes")
			Map spanners = (Map) spannersField.get(driver);
			assertNotNull(spanners);
			assertEquals(1, spanners.size());
		}
	}

}
