package nl.topicus.jdbc.test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

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

	TableDDLTester(Connection connection)
	{
		this.connection = connection;
	}

	void runTests() throws IOException, URISyntaxException, SQLException
	{
		runCreateTableTests();
		runAlterTableTests();
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
		String sql = TestUtil.getResource(getClass(), fileName);
		connection.createStatement().executeUpdate(sql);
		verifyTableExists(tableName);
	}

	private void verifyTableExists(String table) throws SQLException
	{
		String tableFound = "";
		ResultSet rs = connection.getMetaData().getTables("", "", table, null);
		int count = 0;
		while (rs.next())
		{
			tableFound = rs.getString("TABLE_NAME");
			count++;
		}
		if (count != 1 || !table.equalsIgnoreCase(tableFound))
			throw new AssertionError("Table " + table + " not found");
	}

	private void runAlterTableTests()
	{
	}

	private void runDropTableTests()
	{
	}

}
