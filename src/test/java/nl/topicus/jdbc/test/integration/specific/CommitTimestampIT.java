package nl.topicus.jdbc.test.integration.specific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import nl.topicus.jdbc.statement.CloudSpannerPreparedStatement;
import nl.topicus.jdbc.test.category.IntegrationTest;

/**
 * Integration tests for tables with a timestamp column that may contain the
 * commit timestamp column.
 * 
 * @author loite
 *
 */
@Category(IntegrationTest.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CommitTimestampIT extends AbstractSpecificIntegrationTest
{

	@Test
	public void test1_CreateTable() throws SQLException
	{
		// @formatter:off
		String sql = "CREATE TABLE TEST_WITH_COMMIT_TS\n"
				+ "(ID INT64 NOT NULL,\n"
				+ "NAME STRING(100),\n"
				+ "LAST_UPDATED TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true))\n"
				+ "PRIMARY KEY (ID)";
		// @formatter:on
		try (PreparedStatement statement = getConnection().prepareStatement(sql))
		{
			statement.executeUpdate();
		}
		// Verify that the table was created correctly
		try (ResultSet rsTable = getConnection().getMetaData().getTables("", "", "TEST_WITH_COMMIT_TS",
				new String[] { "TABLE" }))
		{
			assertTrue(rsTable.next());
			assertFalse(rsTable.next());
		}
		// Verify the columns
		try (ResultSet rsCols = getConnection().getMetaData().getColumns("", "", "TEST_WITH_COMMIT_TS", null))
		{
			assertTrue(rsCols.next());
			assertEquals("ID", rsCols.getString("COLUMN_NAME"));
			assertEquals("INT64", rsCols.getString("TYPE_NAME"));
			assertEquals(DatabaseMetaData.columnNoNulls, rsCols.getInt("NULLABLE"));

			assertTrue(rsCols.next());
			assertEquals("NAME", rsCols.getString("COLUMN_NAME"));
			assertEquals("STRING(100)", rsCols.getString("TYPE_NAME"));
			assertEquals(DatabaseMetaData.columnNullable, rsCols.getInt("NULLABLE"));

			assertTrue(rsCols.next());
			assertEquals("LAST_UPDATED", rsCols.getString("COLUMN_NAME"));
			assertEquals("TIMESTAMP", rsCols.getString("TYPE_NAME"));
			assertEquals(DatabaseMetaData.columnNoNulls, rsCols.getInt("NULLABLE"));

			assertFalse(rsCols.next());
		}
	}

	@Test
	public void test2_InsertDataWithValueForTimestamp() throws SQLException
	{
		String sql = "INSERT INTO TEST_WITH_COMMIT_TS (ID, NAME, LAST_UPDATED) VALUES (?, ?, ?)";
		try (PreparedStatement statement = getConnection().prepareStatement(sql))
		{
			statement.setLong(1, 1L);
			statement.setString(2, "test");
			statement.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
			statement.executeUpdate();
		}
		getConnection().commit();
		try (ResultSet rs = getConnection().createStatement().executeQuery("SELECT * FROM TEST_WITH_COMMIT_TS"))
		{
			assertTrue(rs.next());
			assertNotNull(rs.getTimestamp("LAST_UPDATED"));
			assertFalse(rs.next());
		}
		getConnection().createStatement().executeUpdate("DELETE FROM TEST_WITH_COMMIT_TS");
		getConnection().commit();
	}

	@Test
	public void test3_InsertDataWithPlaceholderForTimestamp() throws SQLException
	{
		String sql = "INSERT INTO TEST_WITH_COMMIT_TS (ID, NAME, LAST_UPDATED) VALUES (?, ?, ?)";
		try (PreparedStatement statement = getConnection().prepareStatement(sql))
		{
			statement.setLong(1, 1L);
			statement.setString(2, "test");
			statement.setString(3, "spanner.commit_timestamp()");
			statement.executeUpdate();
		}
		getConnection().commit();
		try (ResultSet rs = getConnection().createStatement().executeQuery("SELECT * FROM TEST_WITH_COMMIT_TS"))
		{
			assertTrue(rs.next());
			assertNotNull(rs.getTimestamp("LAST_UPDATED"));
			assertFalse(rs.next());
		}
		getConnection().createStatement().executeUpdate("DELETE FROM TEST_WITH_COMMIT_TS");
		getConnection().commit();
	}

	@Test(expected = SQLException.class)
	public void test4_InsertDataWithNoValueForTimestamp() throws SQLException
	{
		String sql = "INSERT INTO TEST_WITH_COMMIT_TS (ID, NAME) VALUES (?, ?)";
		try (PreparedStatement statement = getConnection().prepareStatement(sql))
		{
			statement.setLong(1, 1L);
			statement.setString(2, "test");
			statement.executeUpdate();
		}
		getConnection().commit();
	}

	@Test(expected = SQLException.class)
	public void test5_InsertDataWithNullForTimestamp() throws SQLException
	{
		String sql = "INSERT INTO TEST_WITH_COMMIT_TS (ID, NAME, LAST_UPDATED) VALUES (?, ?, ?)";
		try (PreparedStatement statement = getConnection().prepareStatement(sql))
		{
			statement.setLong(1, 1L);
			statement.setString(2, "test");
			statement.setNull(3, Types.TIMESTAMP);
			statement.executeUpdate();
		}
		getConnection().commit();
	}

	@Test
	public void test6_InsertDataWithSpecialValueForTimestamp() throws SQLException
	{
		String sql = "INSERT INTO TEST_WITH_COMMIT_TS (ID, NAME, LAST_UPDATED) VALUES (?, ?, ?)";
		try (PreparedStatement statement = getConnection().prepareStatement(sql))
		{
			statement.setLong(1, 1L);
			statement.setString(2, "test");
			statement.setTimestamp(3, CloudSpannerPreparedStatement.SPANNER_COMMIT_TIMESTAMP);
			statement.executeUpdate();
		}
		getConnection().commit();
		try (ResultSet rs = getConnection().createStatement().executeQuery("SELECT * FROM TEST_WITH_COMMIT_TS"))
		{
			assertTrue(rs.next());
			assertNotNull(rs.getTimestamp("LAST_UPDATED"));
			assertFalse(rs.next());
		}
		getConnection().createStatement().executeUpdate("DELETE FROM TEST_WITH_COMMIT_TS");
		getConnection().commit();
	}

}
