package nl.topicus.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.SQLException;
import java.sql.Savepoint;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseClient;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class SavepointTest
{
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private CloudSpannerConnection connection;

	@Before
	public void setup() throws SQLException, NoSuchFieldException, SecurityException, IllegalArgumentException,
			IllegalAccessException
	{
		connection = new CloudSpannerConnection(mock(DatabaseClient.class), mock(BatchClient.class));
		connection.setAutoCommit(false);
	}

	@Test
	public void testSetSavepoint() throws SQLException
	{
		Savepoint savepoint = connection.setSavepoint();
		assertNotNull(savepoint);
		assertNotNull(savepoint.getSavepointId());
	}

	@Test
	public void testSetNamedSavepoint() throws SQLException
	{
		Savepoint savepoint = connection.setSavepoint("test");
		assertNotNull(savepoint);
		assertEquals("test", savepoint.getSavepointName());
	}

	@Test
	public void testReleaseSavepoint() throws SQLException
	{
		connection.createStatement().execute("insert into foo (id, col1) values (1, 'test 1')");
		Savepoint savepoint = connection.setSavepoint();
		connection.createStatement().execute("insert into foo (id, col1) values (2, 'test 2')");
		connection.releaseSavepoint(savepoint);
		thrown.expect(CloudSpannerSQLException.class);
		thrown.expectMessage("Unknown savepoint");
		connection.rollback(savepoint);
	}

	@Test
	public void testReleaseNamedSavepoint() throws SQLException
	{
		connection.createStatement().execute("insert into foo (id, col1) values (1, 'test 1')");
		Savepoint savepoint = connection.setSavepoint("test");
		connection.createStatement().execute("insert into foo (id, col1) values (2, 'test 2')");
		connection.releaseSavepoint(savepoint);
		thrown.expect(CloudSpannerSQLException.class);
		thrown.expectMessage("Unknown savepoint");
		connection.rollback(savepoint);
	}

	@Test
	public void testRollbackSavepoint() throws SQLException
	{
		connection.createStatement().execute("insert into foo (id, col1) values (1, 'test 1')");
		assertEquals(1, connection.getTransaction().getNumberOfBufferedMutations());
		Savepoint savepoint = connection.setSavepoint();
		connection.createStatement().execute("insert into foo (id, col1) values (2, 'test 2')");
		assertEquals(2, connection.getTransaction().getNumberOfBufferedMutations());
		connection.rollback(savepoint);
		assertEquals(1, connection.getTransaction().getNumberOfBufferedMutations());
	}

	@Test
	public void testRollbackNamedSavepoint() throws SQLException
	{
		connection.createStatement().execute("insert into foo (id, col1) values (1, 'test 1')");
		assertEquals(1, connection.getTransaction().getNumberOfBufferedMutations());
		Savepoint savepoint = connection.setSavepoint("test");
		connection.createStatement().execute("insert into foo (id, col1) values (2, 'test 2')");
		assertEquals(2, connection.getTransaction().getNumberOfBufferedMutations());
		connection.rollback(savepoint);
		assertEquals(1, connection.getTransaction().getNumberOfBufferedMutations());
	}

	@Test
	public void testRollbackMixedSavepoints() throws SQLException
	{
		connection.createStatement().execute("insert into foo (id, col1) values (1, 'test 1')");
		assertEquals(1, connection.getTransaction().getNumberOfBufferedMutations());
		Savepoint savepoint1 = connection.setSavepoint();
		connection.createStatement().execute("insert into foo (id, col1) values (2, 'test 2')");
		assertEquals(2, connection.getTransaction().getNumberOfBufferedMutations());
		Savepoint savepoint2 = connection.setSavepoint("2");
		connection.createStatement().execute("insert into foo (id, col1) values (3, 'test 3')");
		assertEquals(3, connection.getTransaction().getNumberOfBufferedMutations());
		Savepoint savepoint3 = connection.setSavepoint("3");
		connection.rollback(savepoint2);
		assertEquals(2, connection.getTransaction().getNumberOfBufferedMutations());
		connection.releaseSavepoint(savepoint1);
		thrown.expect(CloudSpannerSQLException.class);
		thrown.expectMessage("Unknown savepoint");
		connection.releaseSavepoint(savepoint3);
	}

	@Test
	public void testReleaseMultipleSavepoints() throws SQLException
	{
		connection.createStatement().execute("insert into foo (id, col1) values (1, 'test 1')");
		assertEquals(1, connection.getTransaction().getNumberOfBufferedMutations());
		Savepoint savepoint1 = connection.setSavepoint();
		connection.createStatement().execute("insert into foo (id, col1) values (2, 'test 2')");
		assertEquals(2, connection.getTransaction().getNumberOfBufferedMutations());
		connection.setSavepoint("2");
		connection.createStatement().execute("insert into foo (id, col1) values (3, 'test 3')");
		assertEquals(3, connection.getTransaction().getNumberOfBufferedMutations());
		Savepoint savepoint3 = connection.setSavepoint("3");
		connection.releaseSavepoint(savepoint1);
		thrown.expect(CloudSpannerSQLException.class);
		thrown.expectMessage("Unknown savepoint");
		connection.releaseSavepoint(savepoint3);
	}

	@Test
	public void testRollbackMultipleMutations() throws SQLException
	{
		connection.createStatement().execute("insert into foo (id, col1) values (1, 'test 1')");
		Savepoint savepoint = connection.setSavepoint();
		connection.createStatement().execute("insert into foo (id, col1) values (2, 'test 2')");
		connection.createStatement().execute("insert into foo (id, col1) values (3, 'test 3')");
		connection.createStatement().execute("insert into foo (id, col1) values (4, 'test 4')");
		assertEquals(4, connection.getTransaction().getNumberOfBufferedMutations());
		connection.rollback(savepoint);
		assertEquals(1, connection.getTransaction().getNumberOfBufferedMutations());
	}

	@Test
	public void testRollbackToBegin() throws SQLException
	{
		Savepoint savepoint = connection.setSavepoint();
		connection.createStatement().execute("insert into foo (id, col1) values (1, 'test 1')");
		connection.createStatement().execute("insert into foo (id, col1) values (2, 'test 2')");
		connection.createStatement().execute("insert into foo (id, col1) values (3, 'test 3')");
		connection.createStatement().execute("insert into foo (id, col1) values (4, 'test 4')");
		assertEquals(4, connection.getTransaction().getNumberOfBufferedMutations());
		connection.rollback(savepoint);
		assertEquals(0, connection.getTransaction().getNumberOfBufferedMutations());
	}

	@Test
	public void testRollbackToEnd() throws SQLException
	{
		connection.createStatement().execute("insert into foo (id, col1) values (1, 'test 1')");
		connection.createStatement().execute("insert into foo (id, col1) values (2, 'test 2')");
		connection.createStatement().execute("insert into foo (id, col1) values (3, 'test 3')");
		connection.createStatement().execute("insert into foo (id, col1) values (4, 'test 4')");
		Savepoint savepoint = connection.setSavepoint();
		assertEquals(4, connection.getTransaction().getNumberOfBufferedMutations());
		connection.rollback(savepoint);
		assertEquals(4, connection.getTransaction().getNumberOfBufferedMutations());
	}

	@Test
	public void testEqualsAndHashcode() throws SQLException
	{
		Savepoint s1 = connection.setSavepoint();
		Savepoint s2 = connection.setSavepoint();
		assertFalse(s1.equals(s2));
		assertTrue(s1.equals(s1));
		assertTrue(s2.equals(s2));
		assertNotEquals(s1.hashCode(), s2.hashCode());
	}

	@Test
	public void testNamedEqualsAndHashcode() throws SQLException
	{
		Savepoint s1 = connection.setSavepoint("test1");
		Savepoint s2 = connection.setSavepoint("test2");
		assertFalse(s1.equals(s2));
		assertTrue(s1.equals(s1));
		assertTrue(s2.equals(s2));
		assertNotEquals(s1.hashCode(), s2.hashCode());

		s1 = connection.setSavepoint("test");
		s2 = connection.setSavepoint("test");
		assertTrue(s1.equals(s2));
		assertTrue(s1.equals(s1));
		assertTrue(s2.equals(s2));
		assertEquals(s1.hashCode(), s2.hashCode());
	}

}
