package nl.topicus.jdbc.test.ddl;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import nl.topicus.jdbc.test.TestUtil;

/**
 * Class for testing Table DDL statements, such as CREATE TABLE, ALTER TABLE,
 * DROP TABLE.
 * 
 * @author loite
 *
 */
public class TableDDLTester
{
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
		runCreateTableTest("TEST", "CreateTableTest.sql");
		runCreateTableTest("TESTCHILD", "CreateTableTestChild.sql");
	}

	private void runCreateTableTest(String tableName, String fileName) throws IOException, URISyntaxException,
			SQLException
	{
		executeDdl(fileName);
		verifyTableExists(tableName);
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

	private void runAlterTableTests() throws IOException, URISyntaxException, SQLException
	{
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
		String sql = TestUtil.getSingleStatement(getClass(), file);
		try
		{
			connection.createStatement().executeUpdate(sql);
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
