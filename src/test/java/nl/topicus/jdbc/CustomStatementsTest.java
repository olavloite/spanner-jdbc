package nl.topicus.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.internal.stubbing.answers.Returns;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Operation;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;

import nl.topicus.jdbc.CloudSpannerConnection.CloudSpannerDatabaseSpecification;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CustomStatementsTest
{
	private static final List<String> CONNECTION_PROPERTIES = Arrays.asList("AllowExtendedMode", "AsyncDdlOperations",
			"AutoBatchDdlOperations", "ReportDefaultSchemaAsNull", "BatchReadOnlyMode");

	private Connection connection;

	@Before
	public void setup() throws SQLException
	{
		connection = new CloudSpannerConnection(new CloudSpannerDatabaseSpecification("test", "test"));
	}

	@Test
	public void testGetConnectionProperty() throws SQLException
	{
		int count = 0;
		Statement statement = connection.createStatement();
		try (ResultSet rs = statement.executeQuery("GET_CONNECTION_PROPERTY"))
		{
			while (rs.next())
			{
				assertTrue(CONNECTION_PROPERTIES.contains(rs.getString("NAME")));
				count++;
			}
		}
		assertEquals(CONNECTION_PROPERTIES.size(), count);

		for (String prop : CONNECTION_PROPERTIES)
		{
			try (ResultSet rs = statement.executeQuery("GET_CONNECTION_PROPERTY " + prop))
			{
				assertTrue(rs.next());
				assertFalse(rs.next());
			}
		}
	}

	@Test
	public void testSetConnectionProperty() throws SQLException
	{
		connection.setAutoCommit(false);
		Statement statement = connection.createStatement();
		for (String prop : CONNECTION_PROPERTIES)
		{
			for (Boolean value : new Boolean[] { Boolean.TRUE, Boolean.FALSE })
			{
				int count = statement.executeUpdate("SET_CONNECTION_PROPERTY " + prop + "=" + value);
				assertEquals(1, count);
				try (ResultSet rs = statement.executeQuery("GET_CONNECTION_PROPERTY " + prop))
				{
					assertTrue(rs.next());
					assertEquals(value.toString(), rs.getString("VALUE"));
					assertFalse(rs.next());
				}
			}
		}
	}

	@Test
	public void testShowDDLOperations() throws SQLException, NoSuchFieldException, SecurityException,
			IllegalArgumentException, IllegalAccessException
	{
		final String ddl = "CREATE TABLE FOO (ID INT64 NOT NULL, NAME STRING(100) NOT NULL) PRIMARY KEY (ID)";

		Field adminClientField = CloudSpannerConnection.class.getDeclaredField("adminClient");
		adminClientField.setAccessible(true);
		DatabaseAdminClient adminClient = mock(DatabaseAdminClient.class);
		@SuppressWarnings("unchecked")
		Operation<Void, UpdateDatabaseDdlMetadata> operation = mock(Operation.class);
		when(operation.reload()).then(new Returns(operation));
		when(operation.getName()).then(new Returns("test"));
		when(adminClient.updateDatabaseDdl(any(), any(), any(), any())).then(new Returns(operation));
		adminClientField.set(connection, adminClient);
		Statement statement = connection.createStatement();
		assertFalse(statement.execute("SET_CONNECTION_PROPERTY AsyncDdlOperations=true"));
		assertEquals(1, statement.getUpdateCount());
		try (ResultSet rs = statement.executeQuery("SHOW_DDL_OPERATIONS"))
		{
			assertFalse(rs.next());
		}
		statement.execute(ddl);
		try (ResultSet rs = statement.executeQuery("SHOW_DDL_OPERATIONS"))
		{
			assertTrue(rs.next());
			assertEquals("test", rs.getString("NAME"));
			assertNotNull(rs.getTimestamp("TIME_STARTED"));
			assertEquals(ddl, rs.getString("STATEMENT"));
			assertFalse(rs.getBoolean("DONE"));
			assertNull(rs.getString("EXCEPTION"));
			assertFalse(rs.next());
		}
	}

}
