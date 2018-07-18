package nl.topicus.jdbc.test.integration.specific;

import static org.junit.Assert.assertEquals;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import nl.topicus.jdbc.test.category.IntegrationTest;

/**
 * Test cases for different forms of select queries with jdbc parameters in sub
 * select, unions etc.
 * 
 * @author loite
 *
 */
@Category(IntegrationTest.class)
public class SelectStatementsWithParametersIT extends AbstractSpecificIntegrationTest
{

	@Test
	public void testBasicSelect() throws SQLException
	{
		String sql = "select 1 as one, 'two' as two, 3.0 as three";
		testSqlStatement(sql);
	}

	@Test
	public void testSelectWithSubSelect() throws SQLException
	{
		String sql = "select 1 as one, 'two' as two, 3.0 as three from (select 1) sub";
		testSqlStatement(sql);
	}

	@Test
	public void testSelectStarWithSubSelect() throws SQLException
	{
		// @formatter:off
		String sql = "select * \n"
				+ "from (\n"
				+ "  select 1 as one, 'two' as two, 3.0 as three\n"
				+ ") sub\n";
		// @formatter:on
		testSqlStatement(sql);
	}

	@Test
	public void testSelectStarWithSubSelectAndParams() throws SQLException
	{
		// @formatter:off
		String sql = "select * \n"
				+ "from (\n"
				+ "  select 1 as one, 'two' as two, 3.0 as three\n"
				+ ") sub\n"
				+ "where one=?\n"
				+ "and two=?\n"
				+ "and three=?";
		// @formatter:on
		testSqlStatement(sql, 1, 1L, "two", 3.0D);
	}

	@Test
	public void testSelectStarWithSubSelectAndParamsInSubSelect() throws SQLException
	{
		// @formatter:off
		String sql = "select * \n"
				+ "from (\n"
				+ "  select 1 as one, 'two' as two, 3.0 as three\n"
				+ "  from (\n"
				+ "    select 'innersubselect' as inner_col\n"
				+ "  ) sub2"
				+ "  where sub2.inner_col=?"
				+ ") sub1\n"
				+ "where one=?\n"
				+ "and two=?\n"
				+ "and three=?";
		// @formatter:on
		testSqlStatement(sql, 1, "innersubselect", 1L, "two", 3.0D);
		testSqlStatement(sql, 0, "not_found", 1L, "two", 3.0D);
	}

	@Test
	public void testSelectWithParamEqualsSelect() throws SQLException
	{
		String sql = "select 1 as one, 'two' as two, 3.0 as three from (select 1) sub where ?=(select 1)";
		testSqlStatement(sql, 1, 1);
		testSqlStatement(sql, 0, 2);
	}

	private void testSqlStatement(String sql) throws SQLException
	{
		testSqlStatement(sql, 1);
	}

	private void testSqlStatement(String sql, int expectedCount, Object... params) throws SQLException
	{
		PreparedStatement ps = getConnection().prepareStatement(sql);
		if (params != null)
		{
			int index = 1;
			for (Object param : params)
			{
				ps.setObject(index, param);
				index++;
			}
		}
		int count = 0;
		try (ResultSet rs = ps.executeQuery())
		{
			ResultSetMetaData metadata = rs.getMetaData();
			assertEquals("one", metadata.getColumnLabel(1));
			assertEquals("two", metadata.getColumnLabel(2));
			assertEquals("three", metadata.getColumnLabel(3));
			assertEquals(Types.BIGINT, metadata.getColumnType(1));
			assertEquals(Types.NVARCHAR, metadata.getColumnType(2));
			assertEquals(Types.DOUBLE, metadata.getColumnType(3));
			while (rs.next())
			{
				count++;
			}
		}
		assertEquals(expectedCount, count);
	}

}
