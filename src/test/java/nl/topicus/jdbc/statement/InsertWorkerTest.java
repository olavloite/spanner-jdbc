package nl.topicus.jdbc.statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.internal.stubbing.answers.Returns;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.resultset.CloudSpannerResultSet;
import nl.topicus.jdbc.resultset.CloudSpannerResultSetMetaData;
import nl.topicus.jdbc.test.category.UnitTest;
import nl.topicus.jdbc.test.util.CloudSpannerTestObjects;

@Category(UnitTest.class)
public class InsertWorkerTest
{
	private static final boolean WITH_EXCEPTION = true;

	private void createMocks(CloudSpannerConnection connection, String selectSQL, long count, String updateSQL)
			throws SQLException
	{
		createMocks(connection, selectSQL, count, updateSQL, false);
	}

	private void createMocks(CloudSpannerConnection connection, String selectSQL, long count, String updateSQL,
			boolean throwExceptionOnUpdate) throws SQLException
	{
		when(connection.createCopyConnection()).thenAnswer(new Answer<CloudSpannerConnection>()
		{
			@Override
			public CloudSpannerConnection answer(InvocationOnMock invocation) throws Throwable
			{
				CloudSpannerConnection copy = CloudSpannerTestObjects.createConnection();
				createMocks(copy, selectSQL, count, updateSQL);
				return copy;
			}
		});
		CloudSpannerPreparedStatement countStatement = mock(CloudSpannerPreparedStatement.class);
		CloudSpannerResultSet countResultSet = mock(CloudSpannerResultSet.class);
		when(countResultSet.next()).thenReturn(true, false);
		when(countResultSet.getLong(1)).thenReturn(count);
		when(countStatement.executeQuery()).thenReturn(countResultSet);
		when(connection.prepareStatement("SELECT COUNT(*) AS C FROM (" + selectSQL + ") Q")).thenReturn(countStatement);

		CloudSpannerPreparedStatement selectStatement = mock(CloudSpannerPreparedStatement.class);
		CloudSpannerResultSet selectResultSet = mock(CloudSpannerResultSet.class);
		CloudSpannerResultSetMetaData metadata = mock(CloudSpannerResultSetMetaData.class);
		when(metadata.getColumnCount()).thenReturn(3);
		when(selectResultSet.next()).then(new Answer<Boolean>()
		{
			private long called = 0;

			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable
			{
				called++;
				if (called <= count)
					return true;
				return false;
			}
		});
		when(selectResultSet.getObject(1)).then(new Returns(1L));
		when(selectResultSet.getObject(2)).then(new Returns("TWO"));
		when(selectResultSet.getObject(3)).then(new Returns("TO"));
		when(selectResultSet.getMetaData()).thenReturn(metadata);
		when(selectStatement.executeQuery()).thenReturn(selectResultSet);
		when(connection.prepareStatement(selectSQL)).thenReturn(selectStatement);

		CloudSpannerPreparedStatement updateStatement = mock(CloudSpannerPreparedStatement.class);
		if (throwExceptionOnUpdate)
			when(updateStatement.executeUpdate()).thenThrow(SQLException.class);
		else
			when(updateStatement.executeUpdate()).thenReturn(1);
		when(connection.prepareStatement(updateSQL)).thenReturn(updateStatement);
	}

	private CloudSpannerPreparedStatement prepareSimpleInsert() throws SQLException
	{
		return prepareSimpleInsert(false);
	}

	private CloudSpannerPreparedStatement prepareSimpleInsert(boolean exception) throws SQLException
	{
		String sql = "INSERT INTO FOO (ID, COL1, COL2) SELECT 1, 'TWO', 'TO'";
		CloudSpannerPreparedStatement statement = CloudSpannerTestObjects.createPreparedStatement(sql);
		String updateSQL = "INSERT INTO `FOO` (`ID`, `COL1`, `COL2`) VALUES \n(?, ?, ?)";
		createMocks(statement.getConnection(), "SELECT 1, 'TWO', 'TO'", 1l, updateSQL, exception);

		return statement;
	}

	@Test
	public void testSimpleInsertStatement() throws SQLException
	{
		int updates = prepareSimpleInsert().executeUpdate();
		assertEquals(1, updates);
	}

	@Test
	public void testSimpleUpdateStatement() throws SQLException
	{
		String sql = "UPDATE FOO SET COL1='THREE', COL2='TRE' WHERE ID<100";
		CloudSpannerPreparedStatement statement = CloudSpannerTestObjects.createPreparedStatement(sql);
		String updateSQL = "INSERT INTO `FOO` (`ID`, `COL1`, `COL2`) VALUES \n(?, ?, ?) ON DUPLICATE KEY UPDATE";
		createMocks(statement.getConnection(), "SELECT `FOO`.`ID`, 'THREE', 'TRE' FROM `FOO` WHERE ID < 100", 100l,
				updateSQL);
		int updates = statement.executeUpdate();
		assertEquals(100, updates);
	}

	@Test
	public void testExtendedInsertStatement() throws SQLException
	{
		String sql = "INSERT INTO FOO (ID, COL1, COL2) SELECT COL4, COL5, COL6 FROM BAR";
		CloudSpannerPreparedStatement statement = CloudSpannerTestObjects.createPreparedStatement(sql);
		String updateSQL = "INSERT INTO `FOO` (`ID`, `COL1`, `COL2`) VALUES \n(?, ?, ?)";
		createMocks(statement.getConnection(), "SELECT COL4, COL5, COL6 FROM BAR", 6000l, updateSQL);
		int updates = statement.executeUpdate();
		assertEquals(6000, updates);
	}

	@Test
	public void testExtendedUpdateStatement() throws SQLException
	{
		String sql = "UPDATE FOO SET COL1='THREE', COL2='TRE' WHERE ID<6000";
		CloudSpannerPreparedStatement statement = CloudSpannerTestObjects.createPreparedStatement(sql);
		String updateSQL = "INSERT INTO `FOO` (`ID`, `COL1`, `COL2`) VALUES \n(?, ?, ?) ON DUPLICATE KEY UPDATE";
		createMocks(statement.getConnection(), "SELECT `FOO`.`ID`, 'THREE', 'TRE' FROM `FOO` WHERE ID < 6000", 6000l,
				updateSQL);
		int updates = statement.executeUpdate();
		assertEquals(6000, updates);
	}

	private CloudSpannerPreparedStatement prepareAutoCommitSimpleInsert() throws SQLException
	{
		return prepareAutoCommitSimpleInsert(false);
	}

	private CloudSpannerPreparedStatement prepareAutoCommitSimpleInsert(boolean exception) throws SQLException
	{
		CloudSpannerPreparedStatement statement = prepareSimpleInsert(exception);
		CloudSpannerConnection connection = statement.getConnection();
		assertFalse(connection.getAutoCommit());
		connection.setAutoCommit(true);
		assertTrue(connection.getAutoCommit());

		return statement;
	}

	@Test
	public void testSimpleInsertStatementWithAutoCommit() throws SQLException
	{
		CloudSpannerPreparedStatement statement = prepareAutoCommitSimpleInsert();
		int updates = statement.executeUpdate();
		assertEquals(1, updates);
		assertTrue(statement.getConnection().getAutoCommit());
	}

	@Test
	public void testSimpleInsertStatementWithAutoCommitAndException() throws SQLException
	{
		CloudSpannerPreparedStatement statement = prepareAutoCommitSimpleInsert(WITH_EXCEPTION);
		int updates = 0;
		try
		{
			updates = statement.executeUpdate();
		}
		catch (SQLException e)
		{
			assertNotNull(e);
		}
		assertEquals(0, updates);
		assertTrue(statement.getConnection().getAutoCommit());
	}

}
