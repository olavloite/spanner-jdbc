package nl.topicus.jdbc;

import static nl.topicus.jdbc.DDLStatement.Command.CREATE;
import static nl.topicus.jdbc.DDLStatement.Command.DROP;
import static nl.topicus.jdbc.DDLStatement.ExistsStatement.IF_EXISTS;
import static nl.topicus.jdbc.DDLStatement.ExistsStatement.IF_NOT_EXISTS;
import static nl.topicus.jdbc.DDLStatement.ObjectType.INDEX;
import static nl.topicus.jdbc.DDLStatement.ObjectType.TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import nl.topicus.jdbc.DDLStatement.Command;
import nl.topicus.jdbc.DDLStatement.ExistsStatement;
import nl.topicus.jdbc.DDLStatement.ObjectType;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.resultset.CloudSpannerResultSet;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class DDLStatementTest
{
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testParseDDLStatementsCreateTableIfNotExists() throws SQLException
	{
		List<String> sql = Arrays
				.asList("CREATE TABLE IF NOT EXISTS FOO (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		assertEquals(1, statements.size());
		DDLStatement statement = statements.get(0);
		assertStatement(statement, CREATE, TABLE, IF_NOT_EXISTS, "FOO");
		assertEquals("CREATE TABLE   FOO (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)", statement.getSql());
	}

	@Test
	public void testParseDDLStatementsCreateTableQuotedIfNotExists() throws SQLException
	{
		List<String> sql = Arrays
				.asList("CREATE TABLE IF NOT EXISTS `FOO` (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		assertEquals(1, statements.size());
		DDLStatement statement = statements.get(0);
		assertStatement(statement, CREATE, TABLE, IF_NOT_EXISTS, "FOO");
		assertEquals("CREATE TABLE   `FOO` (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)", statement.getSql());
	}

	@Test
	public void testParseDDLStatementsCreateIndexIfNotExists() throws SQLException
	{
		List<String> sql = Arrays.asList("CREATE INDEX IF NOT EXISTS BAR ON FOO (NAME)");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		assertEquals(1, statements.size());
		DDLStatement statement = statements.get(0);
		assertStatement(statement, CREATE, INDEX, IF_NOT_EXISTS, "BAR");
		assertEquals("CREATE INDEX   BAR ON FOO (NAME)", statement.getSql());
	}

	@Test
	public void testParseDDLStatementsCreateIndexQuotedIfNotExists() throws SQLException
	{
		List<String> sql = Arrays.asList("CREATE INDEX IF NOT EXISTS `BAR` ON `FOO` (`NAME`)");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		assertEquals(1, statements.size());
		DDLStatement statement = statements.get(0);
		assertStatement(statement, CREATE, INDEX, IF_NOT_EXISTS, "BAR");
		assertEquals("CREATE INDEX   `BAR` ON `FOO` (`NAME`)", statement.getSql());
	}

	@Test
	public void testParseDDLStatementsCreateUniqueIndexIfNotExists() throws SQLException
	{
		List<String> sql = Arrays.asList("CREATE UNIQUE INDEX IF NOT EXISTS BAR ON FOO (NAME)");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		assertEquals(1, statements.size());
		DDLStatement statement = statements.get(0);
		assertStatement(statement, CREATE, INDEX, IF_NOT_EXISTS, "BAR");
		assertEquals("CREATE UNIQUE INDEX   BAR ON FOO (NAME)", statement.getSql());
	}

	@Test
	public void testParseDDLStatementsCreateNullFilteredIndexIfNotExists() throws SQLException
	{
		List<String> sql = Arrays.asList("CREATE NULL_FILTERED INDEX IF NOT EXISTS BAR ON FOO (NAME)");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		assertEquals(1, statements.size());
		DDLStatement statement = statements.get(0);
		assertStatement(statement, CREATE, INDEX, IF_NOT_EXISTS, "BAR");
		assertEquals("CREATE NULL_FILTERED INDEX   BAR ON FOO (NAME)", statement.getSql());
	}

	@Test
	public void testParseDDLStatementsCreateUniqueNullFilteredIndexIfNotExists() throws SQLException
	{
		List<String> sql = Arrays.asList("CREATE UNIQUE NULL_FILTERED INDEX IF NOT EXISTS BAR ON FOO (NAME)");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		assertEquals(1, statements.size());
		DDLStatement statement = statements.get(0);
		assertStatement(statement, CREATE, INDEX, IF_NOT_EXISTS, "BAR");
		assertEquals("CREATE UNIQUE NULL_FILTERED INDEX   BAR ON FOO (NAME)", statement.getSql());
	}

	@Test
	public void testParseDDLStatementsDropTableIfExists() throws SQLException
	{
		List<String> sql = Arrays.asList("DROP TABLE IF EXISTS FOO");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		assertEquals(1, statements.size());
		DDLStatement statement = statements.get(0);
		assertStatement(statement, DROP, TABLE, IF_EXISTS, "FOO");
		assertEquals("DROP TABLE   FOO", statement.getSql());
	}

	@Test
	public void testParseDDLStatementsDropTableQuotedIfExists() throws SQLException
	{
		List<String> sql = Arrays.asList("DROP TABLE IF EXISTS `FOO`");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		assertEquals(1, statements.size());
		DDLStatement statement = statements.get(0);
		assertStatement(statement, DROP, TABLE, IF_EXISTS, "FOO");
		assertEquals("DROP TABLE   `FOO`", statement.getSql());
	}

	@Test
	public void testParseDDLStatementsDropIndexIfExists() throws SQLException
	{
		List<String> sql = Arrays.asList("DROP INDEX IF EXISTS FOO");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		assertEquals(1, statements.size());
		DDLStatement statement = statements.get(0);
		assertStatement(statement, DROP, INDEX, IF_EXISTS, "FOO");
		assertEquals("DROP INDEX   FOO", statement.getSql());
	}

	@Test
	public void testParseDDLStatementsDropIndexQuotedIfExists() throws SQLException
	{
		List<String> sql = Arrays.asList("DROP INDEX IF EXISTS `FOO`");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		assertEquals(1, statements.size());
		DDLStatement statement = statements.get(0);
		assertStatement(statement, DROP, INDEX, IF_EXISTS, "FOO");
		assertEquals("DROP INDEX   `FOO`", statement.getSql());
	}

	@Test
	public void testParseDDLStatementsCreateTableIfExists() throws SQLException
	{
		List<String> sql = Arrays
				.asList("CREATE TABLE IF EXISTS FOO (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		assertEquals(1, statements.size());
		DDLStatement statement = statements.get(0);
		assertStatement(statement, CREATE, TABLE, IF_EXISTS, "FOO");
		assertEquals("CREATE TABLE   FOO (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)", statement.getSql());
		thrown.expect(CloudSpannerSQLException.class);
		thrown.expectMessage("Invalid argument: Cannot use 'IF EXISTS' when creating an object");
		statement.shouldExecute(null);
	}

	@Test
	public void testParseDDLStatementsDropTableIfNotExists() throws SQLException
	{
		List<String> sql = Arrays.asList("DROP TABLE IF NOT EXISTS FOO");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		assertEquals(1, statements.size());
		DDLStatement statement = statements.get(0);
		assertStatement(statement, DROP, TABLE, IF_NOT_EXISTS, "FOO");
		assertEquals("DROP TABLE   FOO", statement.getSql());
		thrown.expect(CloudSpannerSQLException.class);
		thrown.expectMessage("Invalid argument: Cannot use 'IF NOT EXISTS' when dropping an object");
		statement.shouldExecute(null);
	}

	private CloudSpannerConnection createMockConnection() throws SQLException
	{
		CloudSpannerConnection res = mock(CloudSpannerConnection.class);
		CloudSpannerDatabaseMetaData metadata = mock(CloudSpannerDatabaseMetaData.class);
		when(metadata.getTables("", "", "FOO", null)).then(new Answer<ResultSet>()
		{
			@Override
			public ResultSet answer(InvocationOnMock invocation) throws Throwable
			{
				CloudSpannerResultSet rs = mock(CloudSpannerResultSet.class);
				when(rs.next()).thenReturn(true, false);
				return rs;
			}

		});
		when(metadata.getTables("", "", "TAB", null)).then(new Answer<ResultSet>()
		{
			@Override
			public ResultSet answer(InvocationOnMock invocation) throws Throwable
			{
				CloudSpannerResultSet rs = mock(CloudSpannerResultSet.class);
				when(rs.next()).thenReturn(false);
				return rs;
			}

		});
		when(metadata.getIndexInfo("", "", "BAR")).then(new Answer<ResultSet>()
		{
			@Override
			public ResultSet answer(InvocationOnMock invocation) throws Throwable
			{
				CloudSpannerResultSet rs = mock(CloudSpannerResultSet.class);
				when(rs.next()).thenReturn(true, false);
				return rs;
			}

		});
		when(metadata.getIndexInfo("", "", "IDX")).then(new Answer<ResultSet>()
		{
			@Override
			public ResultSet answer(InvocationOnMock invocation) throws Throwable
			{
				CloudSpannerResultSet rs = mock(CloudSpannerResultSet.class);
				when(rs.next()).thenReturn(false);
				return rs;
			}

		});
		when(res.getMetaData()).thenReturn(metadata);
		return res;
	}

	@Test
	public void testShouldExecuteCreateTableIfNotExistsWhenExists() throws SQLException
	{
		List<String> sql = Arrays
				.asList("CREATE TABLE IF NOT EXISTS FOO (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		DDLStatement statement = statements.get(0);
		assertFalse(statement.shouldExecute(createMockConnection()));
	}

	@Test
	public void testShouldExecuteCreateTableIfNotExistsWhenNotExists() throws SQLException
	{
		List<String> sql = Arrays
				.asList("CREATE TABLE IF NOT EXISTS TAB (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		DDLStatement statement = statements.get(0);
		assertTrue(statement.shouldExecute(createMockConnection()));
	}

	@Test
	public void testShouldExecuteCreateIndexIfNotExistsWhenExists() throws SQLException
	{
		List<String> sql = Arrays.asList("CREATE INDEX IF NOT EXISTS BAR ON FOO (ID)");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		DDLStatement statement = statements.get(0);
		assertFalse(statement.shouldExecute(createMockConnection()));
	}

	@Test
	public void testShouldExecuteCreateIndexIfNotExistsWhenNotExists() throws SQLException
	{
		List<String> sql = Arrays.asList("CREATE INDEX IF NOT EXISTS IDX ON FOO (ID)");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		DDLStatement statement = statements.get(0);
		assertTrue(statement.shouldExecute(createMockConnection()));
	}

	@Test
	public void testShouldExecuteDropTableIfExistsWhenExists() throws SQLException
	{
		List<String> sql = Arrays.asList("DROP TABLE IF EXISTS FOO");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		DDLStatement statement = statements.get(0);
		assertTrue(statement.shouldExecute(createMockConnection()));
	}

	@Test
	public void testShouldExecuteDropTableIfExistsWhenNotExists() throws SQLException
	{
		List<String> sql = Arrays.asList("DROP TABLE IF EXISTS TAB");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		DDLStatement statement = statements.get(0);
		assertFalse(statement.shouldExecute(createMockConnection()));
	}

	@Test
	public void testShouldExecuteDropIndexIfExistsWhenExists() throws SQLException
	{
		List<String> sql = Arrays.asList("DROP INDEX IF EXISTS BAR");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		DDLStatement statement = statements.get(0);
		assertTrue(statement.shouldExecute(createMockConnection()));
	}

	@Test
	public void testShouldExecuteDropIndexIfExistsWhenNotExists() throws SQLException
	{
		List<String> sql = Arrays.asList("DROP INDEX IF EXISTS IDX");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		DDLStatement statement = statements.get(0);
		assertFalse(statement.shouldExecute(createMockConnection()));
	}

	@Test
	public void testParseDDLStatements() throws SQLException
	{
		List<String> sql = Arrays.asList(
				"CREATE TABLE\n\tIF NOT EXISTS FOO\n\t\t(ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)",
				"CREATE          TABLE IF\nNOT\nEXISTS FOO (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)",
				"CREATE TABLE IF\nNOT\nEXISTS\nFOO\n(ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)",
				"CREATE\nTABLE\nIF    NOT  EXISTS   FOO    (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)",
				"CREATE\t TABLE\t IF    NOT EXISTS\tFOO\n(ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)",
				"CREATE\t TABLE\t if not exists\tFOO\n(ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)");
		List<DDLStatement> statements = DDLStatement.parseDdlStatements(sql);
		for (DDLStatement statement : statements)
		{
			assertStatement(statement, CREATE, TABLE, IF_NOT_EXISTS, "FOO");
			assertEquals("CREATE TABLE FOO (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)",
					statement.getSql().trim().replaceAll("\\s+", " "));
		}
	}

	private void assertStatement(DDLStatement statement, Command command, ObjectType objectType, ExistsStatement exists,
			String objectName)
	{
		assertEquals(command, statement.getCommand());
		assertEquals(objectType, statement.getObjectType());
		assertEquals(exists, statement.getExistsStatement());
		assertEquals(objectName, statement.getObjectName());
	}

}
