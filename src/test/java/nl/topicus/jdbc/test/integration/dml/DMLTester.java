package nl.topicus.jdbc.test.integration.dml;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Objects;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import nl.topicus.jdbc.test.integration.TestUtil;

/**
 * Class for testing INSERT, UPDATE, DELETE statements
 * 
 * @author loite
 *
 */
public class DMLTester
{
	private static final Logger log = Logger.getLogger(DMLTester.class.getName());

	private static final int BULK_INSERT_COUNT = 10;

	private static enum ExecuteMode
	{
		Commit
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
				connection.commit();
			}
		},
		Rollback
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
				connection.rollback();
			}
		},
		None
		{
			@Override
			public void execute(Connection connection) throws SQLException
			{
			}
		};

		public abstract void execute(Connection connection) throws SQLException;
	}

	private Connection connection;

	public DMLTester(Connection connection)
	{
		this.connection = connection;
	}

	public void runDMLTests() throws IOException, URISyntaxException, SQLException
	{
		runInsertTests();
		runInsertArrayTests();
		log.info("Verifying table contents after insert");
		verifyTableContentsAfterInsert();

		runInsertOrUpdateTests();
		log.info("Verifying table contents after insert-or-update");
		verifyTableContentsAfterInsertOrUpdate();

		runUpdateTests();
		log.info("Verifying table contents after update");
		verifyTableContentsAfterUpdate();

		log.info("Running specific select tests");
		runSelectTests();

		runDeleteTests();
		runRollbackTests();

		// After rollback test, the contents of the TEST table is equal to the
		// contents after the insert tests.
		runBulkInsertTests();
		runBulkInsertWithExceptionTest();
		runBulkUpdateTests();
		runBulkDeleteTests();
	}

	private void runSelectTests() throws SQLException
	{
		String sql = "SELECT * FROM TESTCHILD@{FORCE_INDEX=IDX_TESTCHILD_DESCRIPTION} WHERE DESCRIPTION LIKE ?";
		PreparedStatement ps = connection.prepareStatement(sql);
		ps.setString(1, "Child%");
		int count = 0;
		try (ResultSet rs = ps.executeQuery())
		{
			while (rs.next())
				count++;
		}
		if (count != 2)
			throw new SQLException("Expected 2 records, found " + count);
	}

	private void runBulkInsertTests() throws SQLException
	{
		verifyTableContentsAfterInsert();
		for (int i = 0; i < BULK_INSERT_COUNT; i++)
		{
			log.info("Starting insert-with-select test no #" + i);
			String sql = "INSERT INTO TEST SELECT ID + (SELECT MAX(ID) FROM TEST), UUID, ACTIVE, AMOUNT, DESCRIPTION, CREATED_DATE, LAST_UPDATED FROM TEST";
			PreparedStatement statement = connection.prepareStatement(sql);
			statement.executeUpdate();
			connection.commit();
			verifyTableContents("SELECT COUNT(*) FROM TEST", Double.valueOf(Math.pow(2, (i + 2))).longValue());
			verifyTableContents("SELECT MAX(ID) FROM TEST", Double.valueOf(Math.pow(2, (i + 2))).longValue());
			log.info("Finished insert-with-select test no #" + i);
		}
	}

	private void runBulkInsertWithExceptionTest() throws SQLException
	{
		log.info("Starting bulk insert with exception test");
		verifyTableContents("SELECT COUNT(*) FROM TEST",
				Double.valueOf(Math.pow(2, (BULK_INSERT_COUNT + 1))).longValue());

		String sql = "INSERT INTO TEST SELECT ID, UUID, ACTIVE, AMOUNT, DESCRIPTION, CREATED_DATE, LAST_UPDATED FROM TEST";
		PreparedStatement statement = connection.prepareStatement(sql);
		try
		{
			statement.executeUpdate();
			connection.commit();
		}
		catch (SQLException e)
		{
			connection.rollback();
		}
		verifyTableContents("SELECT COUNT(*) FROM TEST",
				Double.valueOf(Math.pow(2, (BULK_INSERT_COUNT + 1))).longValue());
		log.info("Finished bulk insert with exception test");
	}

	private void runBulkUpdateTests() throws SQLException
	{
		log.info("Starting bulk update test");
		String sql = "UPDATE TEST SET DESCRIPTION='Divisble by three' WHERE MOD(ID, 3)=0";
		PreparedStatement statement = connection.prepareStatement(sql);
		statement.executeUpdate();
		connection.commit();
		verifyTableContents("SELECT DESCRIPTION FROM TEST WHERE ID=3", "Divisble by three");
		log.info("Finished bulk update test");
	}

	private void runBulkDeleteTests() throws SQLException
	{
		log.info("Starting bulk delete test");
		String sql = "DELETE FROM TEST WHERE MOD(ID, 3)=0";
		PreparedStatement statement = connection.prepareStatement(sql);
		statement.executeUpdate();
		connection.commit();
		verifyTableContents("SELECT COUNT(*) FROM TEST WHERE DESCRIPTION='Divisble by three'", 0L);
		log.info("Finished bulk update test");
	}

	private void runInsertTests() throws IOException, URISyntaxException, SQLException
	{
		log.info("Starting insert tests");
		executeStatements("InsertIntoTest.sql");
		executeStatements("InsertIntoTestChild.sql");
		log.info("Finished insert tests");
	}

	private void runInsertOrUpdateTests() throws IOException, URISyntaxException, SQLException
	{
		log.info("Starting insert-or-update tests");
		executeStatements("InsertOrUpdateIntoTest.sql");
		log.info("Finished insert-or-update tests");
	}

	@SuppressWarnings("deprecation")
	private void runInsertArrayTests() throws IOException, URISyntaxException, SQLException
	{
		log.info("Starting insert array tests");
		executeStatements("InsertIntoTestWithArray.sql", 1,
				connection.createArrayOf("INT64", new Long[] { 1L, 2L, 3L }),
				connection.createArrayOf("BYTES", new byte[][] { { 1, 2, 3 }, { 4, 5, 6 } }),
				connection.createArrayOf("BOOL", new Boolean[] { true, false, true }),
				connection.createArrayOf("FLOAT64", new Double[] { 1D, 2D, 3D }),
				connection.createArrayOf("STRING", new String[] { "foo", "bar" }),
				connection.createArrayOf("DATE",
						new Date[] { new Date(2017 - 1900, 2, 18), new Date(2017 - 1900, 2, 18),
								new Date(2017 - 1900, 2, 18) }),
				connection.createArrayOf("TIMESTAMP",
						new Timestamp[] { new Timestamp(2017 - 1900, 2, 18, 7, 0, 0, 0),
								new Timestamp(2017 - 1900, 2, 18, 7, 0, 0, 0),
								new Timestamp(2017 - 1900, 2, 18, 7, 0, 0, 0) }));
		executeStatements("InsertIntoTestWithArray.sql", 2,
				connection.createArrayOf("INT64", new Long[] { 1L, 2L, 3L }),
				connection.createArrayOf("BYTES", new byte[][] { { 1, 2, 3 }, { 4, 5, 6 } }),
				connection.createArrayOf("BOOL", new Boolean[] { true, false, true }),
				connection.createArrayOf("FLOAT64", new Double[] { 1D, 2D, 3D }),
				connection.createArrayOf("STRING", new String[] { "foo", "bar" }),
				connection.createArrayOf("DATE",
						new Date[] { new Date(2017 - 1900, 2, 18), new Date(2017 - 1900, 2, 18),
								new Date(2017 - 1900, 2, 18) }),
				connection.createArrayOf("TIMESTAMP",
						new Timestamp[] { new Timestamp(2017 - 1900, 2, 18, 7, 0, 0, 0),
								new Timestamp(2017 - 1900, 2, 18, 7, 0, 0, 0),
								new Timestamp(2017 - 1900, 2, 18, 7, 0, 0, 0) }));
		log.info("Finished insert array tests");
	}

	@SuppressWarnings("deprecation")
	private void verifyTableContentsAfterInsert() throws SQLException
	{
		verifyTableContents("SELECT DESCRIPTION FROM TEST WHERE ID=1", "Description 1");
		verifyTableContents("SELECT CREATED_DATE FROM TEST WHERE ID=1", new Date(2017 - 1900, 2, 18));
		verifyTableContents("SELECT LAST_UPDATED FROM TEST WHERE ID=1", new Timestamp(2017 - 1900, 2, 18, 7, 0, 0, 0));
		verifyTableContents("SELECT ACTIVE FROM TEST WHERE ID=1", Boolean.TRUE);

		verifyTableContents("SELECT DESCRIPTION FROM TEST WHERE ID=2", "Description 2");
		verifyTableContents("SELECT AMOUNT FROM TEST WHERE ID=2", Double.valueOf(-29.95d));

		verifyTableContents("SELECT DESCRIPTION FROM TESTCHILD WHERE ID=1 AND CHILDID=1", "Child description 1.1");

		verifyTableContents("SELECT ID FROM TEST_WITH_ARRAY WHERE ID=1", 1L);
		verifyTableContents("SELECT ID2 FROM TEST_WITH_ARRAY WHERE ID=1",
				connection.createArrayOf("INT64", new Long[] { 1L, 2L, 3L }));
		verifyTableContents("SELECT UUID FROM TEST_WITH_ARRAY WHERE ID=1",
				connection.createArrayOf("BYTES", new byte[][] { { 1, 2, 3 }, { 4, 5, 6 } }));
		verifyTableContents("SELECT ACTIVE FROM TEST_WITH_ARRAY WHERE ID=1",
				connection.createArrayOf("BOOL", new Boolean[] { true, false, true }));
		verifyTableContents("SELECT AMOUNT FROM TEST_WITH_ARRAY WHERE ID=1",
				connection.createArrayOf("FLOAT64", new Double[] { 1D, 2D, 3D }));
		verifyTableContents("SELECT DESCRIPTION FROM TEST_WITH_ARRAY WHERE ID=1",
				connection.createArrayOf("STRING", new String[] { "foo", "bar" }));
		verifyTableContents("SELECT CREATED_DATE FROM TEST_WITH_ARRAY WHERE ID=1",
				connection.createArrayOf("DATE", new Date[] { new Date(2017 - 1900, 2, 18),
						new Date(2017 - 1900, 2, 18), new Date(2017 - 1900, 2, 18) }));
		verifyTableContents("SELECT LAST_UPDATED FROM TEST_WITH_ARRAY WHERE ID=1",
				connection.createArrayOf("TIMESTAMP",
						new Timestamp[] { new Timestamp(2017 - 1900, 2, 18, 7, 0, 0, 0),
								new Timestamp(2017 - 1900, 2, 18, 7, 0, 0, 0),
								new Timestamp(2017 - 1900, 2, 18, 7, 0, 0, 0) }));
	}

	private void verifyTableContentsAfterInsertOrUpdate() throws SQLException
	{
		verifyTableContents("SELECT DESCRIPTION FROM TEST WHERE ID=2", "Description 3");
	}

	private void runUpdateTests() throws IOException, URISyntaxException, SQLException
	{
		runUpdateTests(ExecuteMode.Commit);
	}

	private void runUpdateTests(ExecuteMode mode) throws IOException, URISyntaxException, SQLException
	{
		log.info("Starting update tests");
		executeStatements("UpdateTest.sql", mode);
		executeStatements("UpdateTestChild.sql", mode);
		log.info("Finished update tests");
	}

	@SuppressWarnings("deprecation")
	private void verifyTableContentsAfterUpdate() throws SQLException
	{
		verifyTableContents("SELECT UUID FROM TEST WHERE ID=1", DatatypeConverter.parseHexBinary("aabbcc"));
		verifyTableContents("SELECT ACTIVE FROM TEST WHERE ID=1", Boolean.FALSE);
		verifyTableContents("SELECT AMOUNT FROM TEST WHERE ID=1", Double.valueOf(129.95d));

		verifyTableContents("SELECT AMOUNT FROM TEST WHERE ID=2", Double.valueOf(-129.95d));
		verifyTableContents("SELECT CREATED_DATE FROM TEST WHERE ID=2", new Date(2017 - 1900, 1, 17));
		verifyTableContents("SELECT LAST_UPDATED FROM TEST WHERE ID=2", new Timestamp(2017 - 1900, 1, 17, 8, 0, 0, 0));
	}

	private void runDeleteTests() throws IOException, URISyntaxException, SQLException
	{
		log.info("Starting delete tests");
		verifyTableContents("SELECT COUNT(*) FROM TEST", 2L);
		verifyTableContents("SELECT COUNT(*) FROM TESTCHILD", 4L);
		executeStatements("DeleteFromTest.sql");
		verifyTableContents("SELECT COUNT(*) FROM TEST", 1L);
		verifyTableContents("SELECT COUNT(*) FROM TESTCHILD", 2L);
		executeStatements("DeleteFromTestChild.sql");
		verifyTableContents("SELECT COUNT(*) FROM TESTCHILD", 0L);
		executeStatements("DeleteFromTest2.sql");
		verifyTableContents("SELECT COUNT(*) FROM TEST", 0L);
		log.info("Finished delete tests");
	}

	private void runRollbackTests() throws IOException, URISyntaxException, SQLException
	{
		log.info("Starting rollback tests, initializing table contents");
		runInsertTests();
		log.info("Verifying initial table contents");
		verifyTableContentsAfterInsert();

		log.info("Running updates with rollback");
		runUpdateTests(ExecuteMode.Rollback);
		log.info("Verifying unchanged table contents");
		verifyTableContentsAfterInsert();

		log.info("Finished rollback tests");
	}

	private void verifyTableContents(String sql, Object expectedValue) throws SQLException
	{
		try (ResultSet rs = connection.createStatement().executeQuery(sql))
		{
			if (rs.next())
			{
				Object value = rs.getObject(1);
				if (expectedValue != null && expectedValue instanceof byte[])
				{
					if (!Arrays.equals((byte[]) value, (byte[]) expectedValue))
					{
						throw new SQLException("Expected value: " + Arrays.toString((byte[]) expectedValue)
								+ ", found value: " + Arrays.toString((byte[]) value));
					}
				}
				else if (!Objects.equals(value, expectedValue))
				{
					throw new SQLException("Expected value: " + String.valueOf(expectedValue) + ", found value: "
							+ String.valueOf(value));
				}
			}
			else
			{
				throw new SQLException("No records found");
			}
		}
	}

	private void executeStatements(String file, Object... parameters)
			throws IOException, URISyntaxException, SQLException
	{
		executeStatements(file, ExecuteMode.Commit, parameters);
	}

	private void executeStatements(String file, ExecuteMode mode, Object... parameters)
			throws IOException, URISyntaxException, SQLException
	{
		String[] statements = TestUtil.getMultipleStatements(getClass(), file);
		for (String sql : statements)
		{
			if (parameters == null || parameters.length == 0)
			{
				connection.createStatement().executeUpdate(sql);
			}
			else
			{
				executeWithParams(sql, parameters);
			}
		}
		mode.execute(connection);
	}

	private void executeWithParams(String sql, Object... parameters) throws SQLException
	{
		PreparedStatement statement = connection.prepareStatement(sql);
		for (int index = 0; index < parameters.length; index++)
		{
			statement.setObject(index + 1, parameters[index]);
		}
		statement.execute();
	}

}
