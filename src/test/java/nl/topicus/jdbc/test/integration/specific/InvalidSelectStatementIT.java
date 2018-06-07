package nl.topicus.jdbc.test.integration.specific;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import nl.topicus.jdbc.test.category.IntegrationTest;

@Category(IntegrationTest.class)
public class InvalidSelectStatementIT extends AbstractSpecificIntegrationTest
{
	@FunctionalInterface
	private static interface ResultSetFunction<R>
	{
		public R apply(ResultSet t) throws SQLException;
	}

	@Test(expected = SQLException.class)
	public void testSelectFromNonExistentTable() throws SQLException
	{
		runInvalidSelectStatementTest(ResultSet::next);
	}

	@Test(expected = SQLException.class)
	public void testGetMetaDataFromInvalidSelectStatement() throws SQLException
	{
		// This statement should throw a SQL Exception (up and until version
		// 1.0.8 this method would throw a SpannerException)
		runInvalidSelectStatementTest(ResultSet::getMetaData);
	}

	private void runInvalidSelectStatementTest(ResultSetFunction<?> function) throws SQLException
	{
		ResultSet rs = null;
		try
		{
			rs = getConnection().createStatement().executeQuery("SELECT * FROM NON_EXISTENT_TABLE WHERE 1=0");
		}
		catch (SQLException e)
		{
			// should not happen as Cloud Spanner does not actually execute the
			// query before a call to ResultSet#next is executed
			throw new AssertionError(
					"Unexpected SQLException: An invalid query should not throw an exception before any data is actually requested");
		}
		try
		{
			function.apply(rs);
		}
		finally
		{
			// Close the result set
			if (rs != null)
			{
				try
				{
					rs.close();
				}
				catch (SQLException e)
				{
					// Should not happen
					throw new RuntimeException(e);
				}
			}
		}
	}
}
