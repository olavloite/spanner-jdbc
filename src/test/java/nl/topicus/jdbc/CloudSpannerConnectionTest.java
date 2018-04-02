package nl.topicus.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerConnectionTest
{
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private CloudSpannerConnection subject;

	private static final String SIMPLE_SELECT = "SELECT * FROM FOO WHERE ID=?";

	public CloudSpannerConnectionTest() throws SQLException
	{
		subject = createConnection(createDefaultProperties());
	}

	private static Properties createDefaultProperties()
	{
		String project = "test-project-id";
		String instance = "test-instance-id";
		String database = "test-database-id";
		String product = "PostgreSQL";
		String allowExtendedMode = "true";
		Properties properties = new Properties();
		properties.setProperty("Project", project);
		properties.setProperty("Instance", instance);
		properties.setProperty("Database", database);
		properties.setProperty("SimulateProductName", product);
		properties.setProperty("SimulateProductMajorVersion", "9");
		properties.setProperty("SimulateProductMinorVersion", "4");

		properties.setProperty("AllowExtendedMode", allowExtendedMode);

		return properties;
	}

	private static CloudSpannerConnection createConnection(Properties properties) throws SQLException
	{
		String url = "jdbc:cloudspanner://localhost";
		return (CloudSpannerConnection) DriverManager.getConnection(url, properties);
	}

	@Test
	public void testTypeMap() throws Exception
	{
		Map<String, Class<?>> map = subject.getTypeMap();
		assertTrue(map.isEmpty());
		map.put("TEST", Object.class);
		subject.setTypeMap(map);
		assertEquals(1, map.size());
		assertNotNull(subject.getTypeMap());
		assertEquals(1, subject.getTypeMap().size());
	}

	@Test
	public void testProductNameAndVersion() throws SQLException
	{
		Assert.assertEquals("PostgreSQL", subject.getProductName());
		Assert.assertEquals(9, subject.getMetaData().getDatabaseMajorVersion());
		Assert.assertEquals(4, subject.getMetaData().getDatabaseMinorVersion());
		subject.setSimulateProductName(null);
		subject.setSimulateMajorVersion(null);
		subject.setSimulateMinorVersion(null);
		Assert.assertEquals("Google Cloud Spanner", subject.getProductName());
		Assert.assertEquals(1, subject.getMetaData().getDatabaseMajorVersion());
		Assert.assertEquals(0, subject.getMetaData().getDatabaseMinorVersion());
	}

	@Test
	public void testCreateStatement() throws SQLException
	{
		Assert.assertNotNull(subject.createStatement());
		Assert.assertNotNull(subject.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
		Assert.assertNotNull(subject.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
				ResultSet.CLOSE_CURSORS_AT_COMMIT));
	}

	@Test
	public void testPrepareStatement() throws SQLException
	{
		Assert.assertNotNull(subject.prepareStatement(SIMPLE_SELECT));
		Assert.assertNotNull(subject.prepareStatement(SIMPLE_SELECT, Statement.NO_GENERATED_KEYS));
		Assert.assertNotNull(subject.prepareStatement(SIMPLE_SELECT, new int[] { 1, 2 }));
		Assert.assertNotNull(subject.prepareStatement(SIMPLE_SELECT, new String[] { "COL1", "COL2" }));
		Assert.assertNotNull(
				subject.prepareStatement(SIMPLE_SELECT, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
		Assert.assertNotNull(subject.prepareStatement(SIMPLE_SELECT, ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT));
	}

	@Test
	public void testPrepareCall() throws SQLException
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		subject.prepareCall(SIMPLE_SELECT);
	}

	@Test
	public void testNativeSQL() throws SQLException
	{
		String sql = subject.nativeSQL(SIMPLE_SELECT);
		Assert.assertEquals(SIMPLE_SELECT, sql);
	}

	@Test
	public void testAutoCommit() throws SQLException
	{
		Assert.assertTrue(subject.getAutoCommit());
		subject.setAutoCommit(false);
		Assert.assertFalse(subject.getAutoCommit());
		subject.setAutoCommit(true);
		Assert.assertTrue(subject.getAutoCommit());
	}

	@Test
	public void testIsClosed() throws SQLException
	{
		Assert.assertFalse(subject.isClosed());
	}

	@Test
	public void testGetMetaData() throws SQLException
	{
		CloudSpannerDatabaseMetaData metadata = subject.getMetaData();
		Assert.assertNotNull(metadata);
	}

	@Test
	public void testReadOnly() throws SQLException
	{
		Assert.assertFalse(subject.isReadOnly());
		subject.setReadOnly(true);
		Assert.assertTrue(subject.isReadOnly());
		subject.setReadOnly(false);
		Assert.assertFalse(subject.isReadOnly());
	}

	@Test
	public void testTransactionIsolation() throws SQLException
	{
		Assert.assertEquals(Connection.TRANSACTION_SERIALIZABLE, subject.getTransactionIsolation());
		subject.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		thrown.expect(SQLException.class);
		thrown.expectMessage("Only Connection.TRANSACTION_SERIALIZABLE is supported");
		subject.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
	}

	@Test
	public void testGetURL()
	{
		Assert.assertEquals("jdbc:cloudspanner://localhost", subject.getUrl());
	}

	@Test
	public void testGetClientId()
	{
		Assert.assertNull(subject.getClientId());
	}

	@Test
	public void testCreateArrayOf() throws SQLException
	{
		Assert.assertNotNull(subject.createArrayOf("INT64", new Long[] { 1L, 2L, 3L }));
		Assert.assertNotNull(subject.createArrayOf("STRING", new String[] { "one", "two", "three" }));
	}

	@Test
	public void testGetSuppliedProperties()
	{
		Properties properties = subject.getSuppliedProperties();
		Assert.assertEquals("test-project-id", properties.getProperty("Project"));
		Assert.assertEquals("test-instance-id", properties.getProperty("Instance"));
		Assert.assertEquals("test-database-id", properties.getProperty("Database"));
		Assert.assertEquals("PostgreSQL", properties.getProperty("SimulateProductName"));
	}

	@Test
	public void testIsAllowExtendedMode()
	{
		Assert.assertTrue(subject.isAllowExtendedMode());
	}

	@Test
	public void testGetLastCommitTimestamp()
	{
		Assert.assertNull(subject.getLastCommitTimestamp());
	}

	@Test
	public void testOriginalSettings() throws SQLException
	{
		Properties properties = createDefaultProperties();
		CloudSpannerConnection connection = createConnection(properties);
		assertTrue(connection.isOriginalAllowExtendedMode());
		assertFalse(connection.isOriginalAsyncDdlOperations());
		assertFalse(connection.isOriginalAutoBatchDdlOperations());
		assertTrue(connection.isOriginalReportDefaultSchemaAsNull());

		connection.setAllowExtendedMode(false);
		assertTrue(connection.isOriginalAllowExtendedMode());
		assertFalse(connection.isAllowExtendedMode());
		connection.resetDynamicConnectionProperty("AllowExtendedMode");
		assertTrue(connection.isAllowExtendedMode());

		connection.setAsyncDdlOperations(true);
		assertFalse(connection.isOriginalAsyncDdlOperations());
		assertTrue(connection.isAsyncDdlOperations());
		connection.resetDynamicConnectionProperty("AsyncDdlOperations");
		assertFalse(connection.isAsyncDdlOperations());

		connection.setAutoBatchDdlOperations(true);
		assertFalse(connection.isOriginalAutoBatchDdlOperations());
		assertTrue(connection.isAutoBatchDdlOperations());
		connection.resetDynamicConnectionProperty("AutoBatchDdlOperations");
		assertFalse(connection.isAutoBatchDdlOperations());

		connection.setReportDefaultSchemaAsNull(false);
		assertTrue(connection.isOriginalReportDefaultSchemaAsNull());
		assertFalse(connection.isReportDefaultSchemaAsNull());
		connection.resetDynamicConnectionProperty("ReportDefaultSchemaAsNull");
		assertTrue(connection.isReportDefaultSchemaAsNull());

		// Turn off autocommit, otherwise batch read-only mode will fail
		connection.setAutoCommit(false);
		connection.setBatchReadOnly(true);
		assertFalse(connection.isOriginalBatchReadOnly());
		assertTrue(connection.isBatchReadOnly());
		connection.resetDynamicConnectionProperty("BatchReadOnlyMode");
		assertFalse(connection.isBatchReadOnly());
	}

	@Test
	public void testMultipleClosedIsNoOp() throws SQLException
	{
		Properties properties = createDefaultProperties();
		CloudSpannerConnection connection = createConnection(properties);
		connection.close();
		connection.close();
	}

	@Test
	public void testIsValidAfterClose() throws SQLException
	{
		Properties properties = createDefaultProperties();
		CloudSpannerConnection connection = createConnection(properties);
		connection.close();
		assertFalse(connection.isValid(0));
		assertFalse(connection.isValid(1));
	}

	@Test
	public void testGetDynamicConnectionProperties() throws SQLException
	{
		Properties properties = createDefaultProperties();
		try (CloudSpannerConnection connection = createConnection(properties))
		{
			testGetDynamicConnectionProperty(connection, null, 5);
			testGetDynamicConnectionProperty(connection, "ALLOWEXTENDEDMODE", 1);
			testGetDynamicConnectionProperty(connection, "ASYNCDDLOPERATIONS", 1);
			testGetDynamicConnectionProperty(connection, "AUTOBATCHDDLOPERATIONS", 1);
			testGetDynamicConnectionProperty(connection, "REPORTDEFAULTSCHEMAASNULL", 1);
			testGetDynamicConnectionProperty(connection, "BATCHREADONLYMODE", 1);
			testGetDynamicConnectionProperty(connection, "NOT_A_PROPERTY", 0);
		}
	}

	private void testGetDynamicConnectionProperty(Connection connection, String property, int expectedCount)
			throws SQLException
	{
		try (ResultSet rs = connection.createStatement()
				.executeQuery("GET_CONNECTION_PROPERTY" + (property == null ? "" : (" " + property))))
		{
			int count = 0;
			while (rs.next())
				count++;
			assertEquals(expectedCount, count);
		}
	}

	@Test
	public void testClosedAbstractCloudSpannerConnection() throws SQLException, NoSuchMethodException,
			SecurityException, IllegalAccessException, IllegalArgumentException
	{
		testClosed(AbstractCloudSpannerConnection.class, "getCatalog");
		testClosed(AbstractCloudSpannerConnection.class, "getWarnings");
		testClosed(AbstractCloudSpannerConnection.class, "clearWarnings");
		testClosed(AbstractCloudSpannerConnection.class, "getHoldability");
		testClosed(AbstractCloudSpannerConnection.class, "createClob");
		testClosed(AbstractCloudSpannerConnection.class, "createBlob");
		testClosed(AbstractCloudSpannerConnection.class, "createNClob");
		testClosed(AbstractCloudSpannerConnection.class, "createSQLXML");
		testClosed(AbstractCloudSpannerConnection.class, "getCatalog");
		testClosed(AbstractCloudSpannerConnection.class, "getClientInfo");
		testClosed(AbstractCloudSpannerConnection.class, "getSchema");
		testClosed(AbstractCloudSpannerConnection.class, "getNetworkTimeout");

		testClosed(AbstractCloudSpannerConnection.class, "setCatalog", new Class<?>[] { String.class },
				new Object[] { "TEST" });
		testClosed(AbstractCloudSpannerConnection.class, "prepareCall",
				new Class<?>[] { String.class, int.class, int.class }, new Object[] { "TEST", 0, 0 });
		testClosed(AbstractCloudSpannerConnection.class, "prepareCall",
				new Class<?>[] { String.class, int.class, int.class, int.class }, new Object[] { "TEST", 0, 0, 0 });
		testClosed(AbstractCloudSpannerConnection.class, "setClientInfo", new Class<?>[] { String.class, String.class },
				new Object[] { "TEST", "TEST" });
		testClosed(AbstractCloudSpannerConnection.class, "setClientInfo", new Class<?>[] { Properties.class },
				new Object[] { null });
		testClosed(AbstractCloudSpannerConnection.class, "getClientInfo", new Class<?>[] { String.class },
				new Object[] { "TEST" });
		testClosed(AbstractCloudSpannerConnection.class, "createStruct",
				new Class<?>[] { String.class, Object[].class }, new Object[] { "TEST", new Object[] {} });
		testClosed(AbstractCloudSpannerConnection.class, "setSchema", new Class<?>[] { String.class },
				new Object[] { "TEST" });
		testClosed(AbstractCloudSpannerConnection.class, "setNetworkTimeout",
				new Class<?>[] { Executor.class, int.class }, new Object[] { null, 0 });
	}

	@Test
	public void testClosedCloudSpannerConnection() throws SQLException, NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException
	{
		testClosed(CloudSpannerConnection.class, "getTypeMap");
		testClosed(CloudSpannerConnection.class, "createStatement");
		testClosed(CloudSpannerConnection.class, "getAutoCommit");
		testClosed(CloudSpannerConnection.class, "commit");
		testClosed(CloudSpannerConnection.class, "rollback");
		testClosed(CloudSpannerConnection.class, "getMetaData");
		testClosed(CloudSpannerConnection.class, "isReadOnly");
		testClosed(CloudSpannerConnection.class, "getTransactionIsolation");
		testClosed(CloudSpannerConnection.class, "setSavepoint");

		testClosed(CloudSpannerConnection.class, "setTypeMap", new Class<?>[] { Map.class },
				new Object[] { Collections.EMPTY_MAP });
		testClosed(CloudSpannerConnection.class, "prepareStatement", new Class<?>[] { String.class },
				new Object[] { "TEST" });
		testClosed(CloudSpannerConnection.class, "prepareCall", new Class<?>[] { String.class },
				new Object[] { "TEST" });
		testClosed(CloudSpannerConnection.class, "nativeSQL", new Class<?>[] { String.class }, new Object[] { "TEST" });
		testClosed(CloudSpannerConnection.class, "prepareStatement", new Class<?>[] { String.class },
				new Object[] { "TEST" });
		testClosed(CloudSpannerConnection.class, "setAutoCommit", new Class<?>[] { boolean.class },
				new Object[] { true });
		testClosed(CloudSpannerConnection.class, "setReadOnly", new Class<?>[] { boolean.class },
				new Object[] { true });
		testClosed(CloudSpannerConnection.class, "setTransactionIsolation", new Class<?>[] { int.class },
				new Object[] { 0 });
		testClosed(CloudSpannerConnection.class, "createStatement", new Class<?>[] { int.class, int.class },
				new Object[] { 0, 0 });
		testClosed(CloudSpannerConnection.class, "prepareStatement",
				new Class<?>[] { String.class, int.class, int.class }, new Object[] { "TEST", 0, 0 });
		testClosed(CloudSpannerConnection.class, "createStatement", new Class<?>[] { int.class, int.class, int.class },
				new Object[] { 0, 0, 0 });
		testClosed(CloudSpannerConnection.class, "prepareStatement",
				new Class<?>[] { String.class, int.class, int.class, int.class }, new Object[] { "TEST", 0, 0, 0 });
		testClosed(CloudSpannerConnection.class, "prepareStatement", new Class<?>[] { String.class, int.class },
				new Object[] { "TEST", 0 });
		testClosed(CloudSpannerConnection.class, "prepareStatement", new Class<?>[] { String.class, int[].class },
				new Object[] { "TEST", new int[] { 0 } });
		testClosed(CloudSpannerConnection.class, "prepareStatement", new Class<?>[] { String.class, String[].class },
				new Object[] { "TEST", new String[] { "COL1" } });
		testClosed(CloudSpannerConnection.class, "createArrayOf", new Class<?>[] { String.class, Object[].class },
				new Object[] { "TEST", new Object[] { "COL1" } });

		testClosed(CloudSpannerConnection.class, "setSavepoint", new Class<?>[] { String.class },
				new Object[] { "TEST" });
		testClosed(CloudSpannerConnection.class, "rollback", new Class<?>[] { Savepoint.class }, new Object[] { null });
		testClosed(CloudSpannerConnection.class, "releaseSavepoint", new Class<?>[] { Savepoint.class },
				new Object[] { null });
	}

	private void testClosed(Class<? extends AbstractCloudSpannerConnection> clazz, String name)
			throws NoSuchMethodException, SecurityException, SQLException, IllegalAccessException,
			IllegalArgumentException
	{
		testClosed(clazz, name, null, null);
	}

	private void testClosed(Class<? extends AbstractCloudSpannerConnection> clazz, String name, Class<?>[] paramTypes,
			Object[] args) throws NoSuchMethodException, SecurityException, SQLException, IllegalAccessException,
			IllegalArgumentException
	{
		Method method = clazz.getDeclaredMethod(name, paramTypes);
		testInvokeMethodOnClosedConnection(method, args);
	}

	private void testInvokeMethodOnClosedConnection(Method method, Object... args)
			throws SQLException, IllegalAccessException, IllegalArgumentException
	{
		CloudSpannerConnection connection = createConnection(createDefaultProperties());
		connection.close();
		boolean valid = false;
		try
		{
			method.invoke(connection, args);
		}
		catch (InvocationTargetException e)
		{
			if (e.getCause().getMessage().equals(AbstractCloudSpannerConnection.CONNECTION_CLOSED))
			{
				valid = true;
			}
		}
		Assert.assertTrue("Method did not throw exception on closed connection", valid);
	}

}
