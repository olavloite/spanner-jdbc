package nl.topicus.jdbc.test.integration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Test a variety of SELECT-statements
 * 
 * @author loite
 *
 */
public class SelectStatementsTester
{

	private Connection connection;

	public SelectStatementsTester(Connection connection)
	{
		this.connection = connection;
	}

	public void runSelectTests() throws SQLException
	{
		testSelect("SELECT * FROM TEST ORDER BY UUID");
		testSelect("SELECT * FROM TEST ORDER BY UUID DESC");
		testSelect("SELECT ID, UUID, UPDATED FROM TEST ORDER BY UUID DESC, ID");
		testSelect("SELECT * FROM TEST WHERE ID=?", 1L);
		testSelect("SELECT * FROM TEST LIMIT ? OFFSET ? ORDER BY ID", 2L, 10L);
		testSelect("SELECT `ID`, `UUID`, `UPDATED` FROM `TEST` ORDER BY `UUID` DESC, `ID`");
		testSelect("SELECT ID, UUID, UPDATED FROM TEST WHERE ID=? ORDER BY UUID DESC, ID", 1L);
		testSelect("SELECT * FROM TEST WHERE ID IN (SELECT CHILDID FROM TESTCHILD)", 1L);
		testSelect("SELECT * FROM TESTCHILD@{FORCE_INDEX=IDX_TESTCHILD_DESCRIPTION} WHERE DESCRIPTION LIKE ?",
				"%CHILD%");
	}

	private void testSelect(String sql, Object... parameters) throws SQLException
	{
		PreparedStatement ps = connection.prepareStatement(sql);
		for (int i = 1; i <= parameters.length; i++)
		{
			ps.setObject(i, parameters[i - 1]);
		}
		try (ResultSet rs = ps.executeQuery())
		{
		}
	}

}
