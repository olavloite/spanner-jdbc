package nl.topicus.jdbc.test.integration.specific;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.cloud.spanner.AbortedException;
import com.google.rpc.Code;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.test.category.IntegrationTest;

@Category(IntegrationTest.class)
public class TransactionAbortedIT extends AbstractSpecificIntegrationTest
{
	private static final Logger log = Logger.getLogger(TransactionAbortedIT.class.getName());

	@Test
	public void runTransactionAbortedTests() throws SQLException
	{
		if (isRunningOnEmulator())
		{
			createTestTable();
			runTransactionTests();
		}
		else
		{
			log.info("Skipping TransactionAborted tests as the tests are not running on an emulator");
		}
	}

	private void createTestTable() throws SQLException
	{
		String sql = "create table test (id int64 not null, name string(100) not null) primary key (id)";
		getConnection().createStatement().executeUpdate(sql);
	}

	private void runTransactionTests() throws SQLException
	{
		getConnection().createStatement().executeUpdate("insert into test (id, name) values (1, 'one')");
		getConnection().createStatement().executeUpdate("insert into test (id, name) values (2, 'two')");
		getConnection().commit();

		// Start a new transaction and force this one to abort
		// This will still succeed, as the JDBC driver automatically does a
		// retry if an ABORT exception is thrown during a commit
		getConnection().createStatement().executeUpdate("insert into test (id, name) values (3, 'three')");
		try (ResultSet rs = getConnection().createStatement().executeQuery("FORCE_TRANSACTION_ABORT_ON_COMMIT"))
		{
			while (rs.next())
			{
				assertEquals(1L, rs.getLong(1));
			}
		}
		getConnection().commit();

		// start a new transaction.
		getConnection().createStatement().executeUpdate("insert into test (id, name) values (4, 'four')");
		// force it to abort on the next statement
		getConnection().createStatement().executeQuery("FORCE_TRANSACTION_ABORT_ON_COMMIT").next();
		try (ResultSet rs = getConnection().createStatement().executeQuery("select * from test"))
		{
			// This statement (rs.next()) will throw an aborted exception
			while (rs.next())
			{
				log.fine(rs.getLong(1) + " | " + rs.getString(2));
			}
		}
		catch (SQLException e)
		{
			assertEquals(AbortedException.class, e.getCause().getClass());
			log.fine("An expected exception occurred");
		}
		// The following commit will also fail as the transaction had already
		// been aborted
		try
		{
			getConnection().commit();
		}
		catch (CloudSpannerSQLException e)
		{
			assertEquals(Code.FAILED_PRECONDITION, e.getCode());
			assertTrue(e.getMessage().endsWith(
					"The specified transaction was not found or is not the active transaction for this session"));
			log.fine("An expected exception occurred");
		}

		// Try the insert again, but now without an abort
		getConnection().createStatement().executeUpdate("insert into test (id, name) values (4, 'four')");
		getConnection().commit();
	}

}
