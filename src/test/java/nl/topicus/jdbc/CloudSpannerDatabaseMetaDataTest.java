package nl.topicus.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerDatabaseMetaDataTest
{
	private static final String URL = "jdbc:cloudspanner://localhost;Project=adroit-hall-xxx;Instance=test-instance;Database=testdb;PvtKeyPath=C:\\Users\\Olav\\Documents\\CloudSpannerKeys\\cloudspanner3.json;SimulateProductName=PostgreSQL";

	private final CloudSpannerDatabaseMetaData testSubject;

	public CloudSpannerDatabaseMetaDataTest() throws SQLException
	{
		testSubject = createTestSubject(URL);
	}

	private CloudSpannerDatabaseMetaData createTestSubject(String url) throws SQLException
	{
		CloudSpannerConnection connection = MockCloudSpannerConnection.create(url);
		return new CloudSpannerDatabaseMetaData(connection);
	}

	@Test
	public void testAllProceduresAreCallable() throws SQLException
	{
		assertTrue(testSubject.allProceduresAreCallable());
	}

	@Test
	public void testAllTablesAreSelectable() throws SQLException
	{
		assertTrue(testSubject.allTablesAreSelectable());
	}

	@Test
	public void testGetURL() throws SQLException
	{
		assertEquals(URL, testSubject.getURL());
	}

	@Test
	public void testGetUserName() throws SQLException
	{
		assertNull(testSubject.getUserName());
	}

	@Test
	public void testIsReadOnly() throws SQLException
	{
		assertEquals(false, testSubject.isReadOnly());
	}

	@Test
	public void testNullsAreSortedHigh() throws SQLException
	{
		assertEquals(false, testSubject.nullsAreSortedHigh());
	}

	@Test
	public void testNullsAreSortedLow() throws SQLException
	{
		assertTrue(testSubject.nullsAreSortedLow());
	}

	@Test
	public void testNullsAreSortedAtStart() throws SQLException
	{
		assertTrue(testSubject.nullsAreSortedAtStart());
	}

	@Test
	public void testNullsAreSortedAtEnd() throws SQLException
	{
		assertEquals(false, testSubject.nullsAreSortedAtEnd());
	}

	@Test
	public void testGetDatabaseProductName() throws SQLException
	{
		assertEquals("PostgreSQL", testSubject.getDatabaseProductName());
	}

	@Test
	public void testGetDatabaseProductVersion() throws SQLException
	{
		assertEquals("1.0", testSubject.getDatabaseProductVersion());
	}

	@Test
	public void testGetDriverName() throws SQLException
	{
		assertEquals(CloudSpannerDriver.class.getName(), testSubject.getDriverName());
	}

	@Test
	public void testGetDriverVersion() throws SQLException
	{
		assertEquals(CloudSpannerDriver.MAJOR_VERSION + "." + CloudSpannerDriver.MINOR_VERSION,
				testSubject.getDriverVersion());
	}

	@Test
	public void testGetDriverMajorVersion()
	{
		assertEquals(CloudSpannerDriver.MAJOR_VERSION, testSubject.getDriverMajorVersion());
	}

	@Test
	public void testGetDriverMinorVersion()
	{
		assertEquals(CloudSpannerDriver.MINOR_VERSION, testSubject.getDriverMinorVersion());
	}

	@Test
	public void testUsesLocalFiles() throws SQLException
	{
		assertEquals(false, testSubject.usesLocalFiles());
	}

	@Test
	public void testUsesLocalFilePerTable() throws SQLException
	{
		assertEquals(false, testSubject.usesLocalFilePerTable());
	}

	@Test
	public void testSupportsMixedCaseIdentifiers() throws SQLException
	{
		assertEquals(false, testSubject.supportsMixedCaseIdentifiers());
	}

}