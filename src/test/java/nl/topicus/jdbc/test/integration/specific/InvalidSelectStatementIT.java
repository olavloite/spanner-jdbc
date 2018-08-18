package nl.topicus.jdbc.test.integration.specific;

import static org.junit.Assert.fail;
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
		try(ResultSet rs = getConnection().createStatement().executeQuery("SELECT * FROM NON_EXISTENT_TABLE WHERE 1=0"))
		{
	        fail(
	            "Unexpected program flow: An invalid query should throw an exception directly when it is executed");
	        function.apply(rs);
		}
	}
}
