package nl.topicus.jdbc.test.integration.dml;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Date;
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
		log.info("Verifying table contents");
		verifyTableContentsAfterInsert();

		runUpdateTests();
		log.info("Verifying table contents");
		verifyTableContentsAfterUpdate();

		runDeleteTests();
		runRollbackTests();
	}

	private void runInsertTests() throws IOException, URISyntaxException, SQLException
	{
		log.info("Starting insert tests");
		executeStatements("InsertIntoTest.sql");
		executeStatements("InsertIntoTestChild.sql");
		log.info("Finished insert tests");
	}

	@SuppressWarnings("deprecation")
	private void verifyTableContentsAfterInsert() throws SQLException
	{
		verifyTableContents("SELECT DESCRIPTION FROM TEST WHERE ID=1", "Description 1");
		verifyTableContents("SELECT CREATED_DATE FROM TEST WHERE ID=1", new Date(2017 - 1900, 2, 18));
		verifyTableContents("SELECT LAST_UPDATED FROM TEST WHERE ID=1", new Timestamp(2017 - 1900, 2, 18, 7, 0, 0, 0));
		verifyTableContents("SELECT ACTIVE FROM TEST WHERE ID=1", Boolean.TRUE);

		verifyTableContents("SELECT DESCRIPTION FROM TEST WHERE ID=2", "Description 2");
		verifyTableContents("SELECT AMOUNT FROM TEST WHERE ID=2", Float.valueOf(-29.95f));

		verifyTableContents("SELECT DESCRIPTION FROM TESTCHILD WHERE ID=1 AND CHILDID=1", "Child description 1.1");
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
		verifyTableContents("SELECT AMOUNT FROM TEST WHERE ID=1", Float.valueOf(129.95f));

		verifyTableContents("SELECT AMOUNT FROM TEST WHERE ID=2", Float.valueOf(-129.95f));
		verifyTableContents("SELECT CREATED_DATE FROM TEST WHERE ID=2", new Date(2017 - 1900, 1, 17));
		verifyTableContents("SELECT LAST_UPDATED FROM TEST WHERE ID=2", new Timestamp(2017 - 1900, 1, 17, 8, 0, 0, 0));
	}

	private void runDeleteTests() throws IOException, URISyntaxException, SQLException
	{
		log.info("Starting delete tests");
		verifyTableContents("SELECT COUNT(*) FROM TEST", 2);
		verifyTableContents("SELECT COUNT(*) FROM TESTCHILD", 4);
		executeStatements("DeleteFromTest.sql");
		verifyTableContents("SELECT COUNT(*) FROM TEST", 1);
		verifyTableContents("SELECT COUNT(*) FROM TESTCHILD", 2);
		executeStatements("DeleteFromTest2.sql");
		verifyTableContents("SELECT COUNT(*) FROM TEST", 0);
		verifyTableContents("SELECT COUNT(*) FROM TESTCHILD", 0);
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

	private void executeStatements(String file) throws IOException, URISyntaxException, SQLException
	{
		executeStatements(file, ExecuteMode.Commit);
	}

	private void executeStatements(String file, ExecuteMode mode) throws IOException, URISyntaxException, SQLException
	{
		String[] statements = TestUtil.getMultipleStatements(getClass(), file);
		for (String sql : statements)
		{
			connection.createStatement().executeUpdate(sql);
		}
		mode.execute(connection);
	}

}
