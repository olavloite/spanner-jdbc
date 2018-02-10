package nl.topicus.jdbc.test.integration.ddl;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import org.junit.Assert;

import nl.topicus.jdbc.test.integration.TestUtil;

/**
 * Class for testing Table DDL statements, such as CREATE TABLE, ALTER TABLE,
 * DROP TABLE.
 * 
 * @author loite
 *
 */
public class TableDDLTester
{
	private static final Logger log = Logger.getLogger(TableDDLTester.class.getName());

	private Connection connection;

	public TableDDLTester(Connection connection)
	{
		this.connection = connection;
	}

	public void runCreateTests() throws IOException, URISyntaxException, SQLException
	{
		runCreateTableTests();
		runAlterTableTests();
	}

	public void runDropTests()
	{
		runDropTableTests();
	}

	private void runCreateTableTests() throws IOException, URISyntaxException, SQLException
	{
		log.info("Starting CreateTableTests");
		runCreateTableTest("TEST", "CreateTableTest.sql");
		log.info("Starting CreateTableIfNotExistsTests");
		runCreateTableTest("TEST", "CreateTableIfNotExistsTest.sql");
		log.info("Verifying primary keys of parent table");
		verifyPrimaryKey("TEST", "ID");
		runCreateTableTest("TESTCHILD", "CreateTableTestChild.sql");
		log.info("Verifying primary keys of child table");
		verifyPrimaryKey("TESTCHILD", "ID, CHILDID");
		log.info("Creating indices");
		runCreateIndexTests();

		log.info("Starting CreateQuotedTableTest");
		runCreateTableTest("TEST_QUOTED", "CreateQuotedTableTest.sql");
		log.info("Starting CreateTableWithArray");
		runCreateTableTest("TEST_WITH_ARRAY", "CreateTableWithArray.sql");

		log.info("Starting CreateAsyncTableTest");
		runCreateTableTest("ASYNCTEST", "CreateTableAsyncTest.sql");
		for (int expectedCount = 1; expectedCount >= 0; expectedCount--)
		{
			try (ResultSet rs = connection.createStatement().executeQuery("SHOW_DDL_OPERATIONS"))
			{
				int count = 0;
				while (rs.next())
				{
					count++;
					Assert.assertTrue(rs.getBoolean("DONE"));
				}
				Assert.assertEquals("Number of running operations", expectedCount, count);
				connection.createStatement().executeUpdate("CLEAN_DDL_OPERATIONS");
			}
		}

		log.info("Finished CreateTableTests");
	}

	private void runCreateTableTest(String tableName, String fileName)
			throws IOException, URISyntaxException, SQLException
	{
		executeDdl(fileName);
		verifyTableExists(tableName);
	}

	private void runCreateIndexTests() throws IOException, URISyntaxException, SQLException
	{
		// CREATE UNIQUE INDEX takes forever, and also hinders bulk insert tests
		// executeDdl("CreateIndexTest.sql");
		executeDdl("CreateIndexTestChild.sql");
		executeDdl("CreateIndexIfNotExistsTestChild.sql");
	}

	private void verifyTableExists(String table) throws SQLException
	{
		String tableFound = "";
		try (ResultSet rs = connection.getMetaData().getTables("", "", table, null))
		{
			int count = 0;
			while (rs.next())
			{
				tableFound = rs.getString("TABLE_NAME");
				count++;
			}
			if (count != 1 || !table.equalsIgnoreCase(tableFound))
				throw new AssertionError("Table " + table + " not found");
		}
	}

	private void verifyPrimaryKey(String table, String expectedColumns) throws SQLException
	{
		String colsFound = "";
		try (ResultSet rs = connection.getMetaData().getPrimaryKeys("", "", table))
		{
			while (rs.next())
			{
				if (!colsFound.equals(""))
					colsFound = colsFound + ", ";
				colsFound = colsFound + rs.getString("COLUMN_NAME");
			}
			if (!expectedColumns.equalsIgnoreCase(colsFound))
				throw new AssertionError("Primary key columns expected: " + expectedColumns + ", found: " + colsFound);
		}
	}

	private void runAlterTableTests() throws IOException, URISyntaxException, SQLException
	{
		log.info("Starting AlterTableTests");
		executeDdl("AlterTableTestChildAddColumn.sql");
		verifyColumn("TESTCHILD", "NEW_COLUMN", true, "STRING(50)");

		// This statement will fail as it is not allowed to change the type from
		// STRING(50) to INT64
		executeDdl("AlterTableTestChildAlterColumn.sql", true);
		verifyColumn("TESTCHILD", "NEW_COLUMN", true, "STRING(50)");

		executeDdl("AlterTableTestChildAlterColumn2.sql");
		verifyColumn("TESTCHILD", "NEW_COLUMN", true, "STRING(100)");

		executeDdl("AlterTableTestChildDropColumn.sql");
		verifyColumn("TESTCHILD", "NEW_COLUMN", false, "");
		log.info("Finished AlterTableTests");
	}

	private void verifyColumn(String table, String column, boolean mustExist, String type) throws SQLException
	{
		String colFound = "";
		String typeFound = "";
		ResultSet rs = connection.getMetaData().getColumns("", "", table, column);
		int count = 0;
		while (rs.next())
		{
			colFound = rs.getString("COLUMN_NAME");
			typeFound = rs.getString("TYPE_NAME");
			count++;
		}
		if (mustExist)
		{
			if (count != 1 || !column.equalsIgnoreCase(colFound))
				throw new AssertionError("Column " + column + " not found");
			if (!type.equalsIgnoreCase(typeFound))
				throw new AssertionError("Column " + column + " has type " + typeFound + ", expected was " + type);
		}
		else
		{
			if (count != 0)
				throw new AssertionError("Column " + column + " found, expected was no column");
		}
	}

	private void runDropTableTests()
	{
	}

	private void executeDdl(String file) throws IOException, URISyntaxException, SQLException
	{
		executeDdl(file, false);
	}

	private void executeDdl(String file, boolean expectsError) throws IOException, URISyntaxException, SQLException
	{
		String[] sqls = TestUtil.getMultipleStatements(getClass(), file);
		try
		{
			for (String sql : sqls)
				connection.createStatement().execute(sql);
		}
		catch (SQLException e)
		{
			if (!expectsError)
				throw e;
			return;
		}
		if (expectsError)
			throw new SQLException("Expected exception, but statement was succesfull");
	}

}
