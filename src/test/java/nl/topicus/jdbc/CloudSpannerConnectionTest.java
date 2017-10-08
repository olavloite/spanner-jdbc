package nl.topicus.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;

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
		String url = "jdbc:cloudspanner://localhost";
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
		properties.setProperty("AllowExtendedMode", allowExtendedMode);
		subject = (CloudSpannerConnection) DriverManager.getConnection(url, properties);
	}

	@Test
	public void testProductName()
	{
		Assert.assertEquals("PostgreSQL", subject.getProductName());
		subject.setSimulateProductName(null);
		Assert.assertEquals("Google Cloud Spanner", subject.getProductName());
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

}
