package nl.topicus.jdbc;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import nl.topicus.jdbc.CloudSpannerConnection.CloudSpannerDatabaseSpecification;

public class AbstractCloudSpannerConnectionTest
{
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private AbstractCloudSpannerConnection createTestSubject() throws SQLException
	{
		Properties props = new Properties();
		props.setProperty("Project", "test");
		props.setProperty("Instance", "test");
		props.setProperty("Database", "test");
		return new CloudSpannerConnection((CloudSpannerDriver) DriverManager.getDriver("jdbc:cloudspanner://localhost"),
				"jdbc:cloudspanner://localhost", new CloudSpannerDatabaseSpecification("test", "test", "test"), null,
				"oauth", props);
	}

	@Test
	public void testSetCatalog() throws Exception
	{
		AbstractCloudSpannerConnection testSubject;
		String catalog = "";

		// default test
		testSubject = createTestSubject();
		testSubject.setCatalog(catalog);
	}

	@Test
	public void testGetCatalog() throws Exception
	{
		AbstractCloudSpannerConnection testSubject = createTestSubject();
		Assert.assertNull(testSubject.getCatalog());
		testSubject.setReportDefaultSchemaAsNull(false);
		Assert.assertEquals("", testSubject.getCatalog());
	}

	@Test
	public void testGetWarnings() throws Exception
	{
		AbstractCloudSpannerConnection testSubject;
		SQLWarning result;

		// default test
		testSubject = createTestSubject();
		result = testSubject.getWarnings();
		Assert.assertNull(result);
	}

	@Test
	public void testClearWarnings() throws Exception
	{
		AbstractCloudSpannerConnection testSubject;

		// default test
		testSubject = createTestSubject();
		testSubject.clearWarnings();
	}

	@Test
	public void testPrepareCall() throws Exception
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		AbstractCloudSpannerConnection testSubject;
		String sql = "";
		int resultSetType = 0;
		int resultSetConcurrency = 0;

		// default test
		testSubject = createTestSubject();
		testSubject.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	@Test
	public void testSetHoldability() throws Exception
	{
		AbstractCloudSpannerConnection testSubject;
		int holdability = ResultSet.CLOSE_CURSORS_AT_COMMIT;

		testSubject = createTestSubject();
		testSubject.setHoldability(holdability);

		thrown.expect(SQLFeatureNotSupportedException.class);
		testSubject.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
	}

	@Test
	public void testGetHoldability() throws Exception
	{
		AbstractCloudSpannerConnection testSubject;
		int result;

		// default test
		testSubject = createTestSubject();
		result = testSubject.getHoldability();
		Assert.assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, result);
	}

	@Test
	public void testPrepareCall_1() throws Exception
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		AbstractCloudSpannerConnection testSubject;
		String sql = "";
		int resultSetType = 0;
		int resultSetConcurrency = 0;
		int resultSetHoldability = 0;

		// default test
		testSubject = createTestSubject();
		testSubject.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Test
	public void testPrepareCall_2() throws SQLException
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		AbstractCloudSpannerConnection testSubject = createTestSubject();
		testSubject.prepareCall("", 0, 0);
	}

	@Test
	public void testCreateClob() throws Exception
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		AbstractCloudSpannerConnection testSubject;

		// default test
		testSubject = createTestSubject();
		testSubject.createClob();
	}

	@Test
	public void testCreateBlob() throws Exception
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		AbstractCloudSpannerConnection testSubject;

		// default test
		testSubject = createTestSubject();
		testSubject.createBlob();
	}

	@Test
	public void testCreateNClob() throws Exception
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		AbstractCloudSpannerConnection testSubject;

		// default test
		testSubject = createTestSubject();
		testSubject.createNClob();
	}

	@Test
	public void testCreateSQLXML() throws Exception
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		AbstractCloudSpannerConnection testSubject;

		// default test
		testSubject = createTestSubject();
		testSubject.createSQLXML();
	}

	@Test
	public void testSetClientInfo() throws Exception
	{
		AbstractCloudSpannerConnection testSubject;
		String name = "";
		String value = "";

		// default test
		testSubject = createTestSubject();
		testSubject.setClientInfo(name, value);
	}

	@Test
	public void testSetClientInfo_1() throws Exception
	{
		AbstractCloudSpannerConnection testSubject;
		Properties properties = null;

		// default test
		testSubject = createTestSubject();
		testSubject.setClientInfo(properties);
	}

	@Test
	public void testGetClientInfo() throws Exception
	{
		AbstractCloudSpannerConnection testSubject;
		String name = "";
		String result;

		// default test
		testSubject = createTestSubject();
		result = testSubject.getClientInfo(name);
		Assert.assertNull(result);
	}

	@Test
	public void testGetClientInfo_1() throws Exception
	{
		AbstractCloudSpannerConnection testSubject;
		Properties result;

		// default test
		testSubject = createTestSubject();
		result = testSubject.getClientInfo();
		Assert.assertNotNull(result);
		Assert.assertEquals(0, result.size());
	}

	@Test
	public void testCreateStruct() throws Exception
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		AbstractCloudSpannerConnection testSubject;
		String typeName = "";
		Object[] attributes = new Object[] { null };

		// default test
		testSubject = createTestSubject();
		testSubject.createStruct(typeName, attributes);
	}

	@Test
	public void testSetSchema() throws Exception
	{
		AbstractCloudSpannerConnection testSubject;
		String schema = "";

		// default test
		testSubject = createTestSubject();
		testSubject.setSchema(schema);
	}

	@Test
	public void testGetSchema() throws Exception
	{
		AbstractCloudSpannerConnection testSubject = createTestSubject();
		Assert.assertNull(testSubject.getSchema());
		testSubject.setReportDefaultSchemaAsNull(false);
		Assert.assertEquals("", testSubject.getSchema());
	}

	@Test
	public void testAbort() throws Exception
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		AbstractCloudSpannerConnection testSubject;
		Executor executor = null;

		// default test
		testSubject = createTestSubject();
		testSubject.abort(executor);
	}

	@Test
	public void testSetNetworkTimeout() throws Exception
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		AbstractCloudSpannerConnection testSubject;
		Executor executor = null;
		int milliseconds = 0;

		// default test
		testSubject = createTestSubject();
		testSubject.setNetworkTimeout(executor, milliseconds);
	}

	@Test
	public void testGetNetworkTimeout() throws Exception
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		AbstractCloudSpannerConnection testSubject;

		// default test
		testSubject = createTestSubject();
		testSubject.getNetworkTimeout();
	}
}