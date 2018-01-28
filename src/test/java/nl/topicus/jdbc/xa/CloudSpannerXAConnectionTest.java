package nl.topicus.jdbc.xa;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Random;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.ICloudSpannerConnection;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.test.category.UnitTest;
import nl.topicus.jdbc.test.util.CloudSpannerTestObjects;

@Category(UnitTest.class)
public class CloudSpannerXAConnectionTest
{
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private Random random = new Random();

	private CloudSpannerXAConnection createSubject() throws SQLException
	{
		CloudSpannerConnection conn = CloudSpannerTestObjects.createConnection();
		CloudSpannerXAConnection res = new CloudSpannerXAConnection(conn);
		return res;
	}

	private Xid createXid()
	{
		int id = random.nextInt();
		return RecoveredXid.stringToXid(String.valueOf(id) + "_Z3RyaWQ=_YnF1YWw=");
	}

	@Test
	public void testGetConnection() throws SQLException
	{
		CloudSpannerXAConnection subject = createSubject();
		ICloudSpannerConnection connection = subject.getConnection();
		assertNotNull(connection);
		assertTrue(connection.getAutoCommit());
		assertEquals(subject.getXAResource(), subject);
		connection.setAutoCommit(false);
		assertFalse(connection.getAutoCommit());
	}

	@Test
	public void testStart() throws SQLException, XAException
	{
		CloudSpannerXAConnection subject = createSubject();
		ICloudSpannerConnection connection = subject.getConnection();
		assertTrue(connection.getAutoCommit());
		Xid xid = createXid();
		subject.start(xid, XAResource.TMNOFLAGS);
		assertNotNull(connection);
		assertFalse(connection.getAutoCommit());
		thrown.expect(CloudSpannerSQLException.class);
		connection.commit();
	}

	@Test
	public void testEnd() throws SQLException, XAException
	{
		CloudSpannerXAConnection subject = createSubject();
		Xid xid = createXid();
		subject.start(xid, XAResource.TMNOFLAGS);
		subject.end(xid, XAResource.TMSUCCESS);
	}

	@Test
	public void testPrepare() throws SQLException, XAException
	{
		CloudSpannerXAConnection subject = createSubject();
		Xid xid = createXid();
		subject.start(xid, XAResource.TMNOFLAGS);
		subject.end(xid, XAResource.TMSUCCESS);
		subject.prepare(xid);
	}

	@Test
	public void testCommitOnePhase() throws SQLException, XAException
	{
		CloudSpannerXAConnection subject = createSubject();
		Xid xid = createXid();
		subject.start(xid, XAResource.TMNOFLAGS);
		subject.end(xid, XAResource.TMSUCCESS);
		subject.commit(xid, true);
	}

	@Test
	public void testCommitTwoPhase() throws SQLException, XAException
	{
		CloudSpannerXAConnection subject = createSubject();
		Xid xid = createXid();
		subject.start(xid, XAResource.TMNOFLAGS);
		subject.end(xid, XAResource.TMSUCCESS);
		subject.prepare(xid);
		subject.commit(xid, false);
	}

	@FunctionalInterface
	private static interface RunnableWithException
	{
		public abstract void run() throws Exception;
	}

	@Test
	public void testExpectedErrors() throws SQLException
	{
		CloudSpannerXAConnection subject = createSubject();
		testExpectedError(() -> subject.getConnection().prepareCall("TEST"), SQLFeatureNotSupportedException.class);
		testExpectedError(() -> subject.start(getRandomXid(), XAResource.TMSUCCESS), CloudSpannerXAException.class,
				CloudSpannerXAException.INVALID_FLAGS);
		testExpectedError(() -> subject.start(null, XAResource.TMNOFLAGS), CloudSpannerXAException.class,
				CloudSpannerXAException.XID_NOT_NULL);
		testExpectedError(() -> subject.start(getRandomXid(), XAResource.TMRESUME), CloudSpannerXAException.class,
				CloudSpannerXAException.SUSPEND_NOT_IMPLEMENTED);

		testExpectedError(() -> subject.end(getRandomXid(), XAResource.TMNOFLAGS), CloudSpannerXAException.class,
				CloudSpannerXAException.INVALID_FLAGS);
		testExpectedError(() -> subject.end(null, XAResource.TMSUCCESS), CloudSpannerXAException.class,
				CloudSpannerXAException.XID_NOT_NULL);

		// Get a second connection and test the equals method
		CloudSpannerXAConnection secondConnection = createSubject();
		Assert.assertNotEquals(subject.getConnection(), secondConnection.getConnection());
	}

	@Test
	public void testStartWhenActive() throws SQLException, XAException
	{
		thrown.expect(CloudSpannerXAException.class);
		thrown.expectMessage(CloudSpannerXAException.CONNECTION_BUSY);
		CloudSpannerXAConnection subject = createSubject();
		subject.start(getRandomXid(), XAResource.TMNOFLAGS);
		subject.start(getRandomXid(), XAResource.TMNOFLAGS);
	}

	@Test
	public void testJoin() throws SQLException, XAException
	{
		CloudSpannerXAConnection subject = createSubject();
		Xid xid = getRandomXid();
		subject.start(xid, XAResource.TMNOFLAGS);
		subject.end(xid, XAResource.TMSUCCESS);
		subject.start(xid, XAResource.TMJOIN);
	}

	@Test
	public void testJoinWhenNotEnded() throws SQLException, XAException
	{
		thrown.expect(CloudSpannerXAException.class);
		thrown.expectMessage(CloudSpannerXAException.INTERLEAVING_NOT_IMPLEMENTED);
		CloudSpannerXAConnection subject = createSubject();
		Xid xid = getRandomXid();
		subject.start(xid, XAResource.TMJOIN);
	}

	@Test
	public void testJoinWithDifferentXid() throws SQLException, XAException
	{
		thrown.expect(CloudSpannerXAException.class);
		thrown.expectMessage(CloudSpannerXAException.INTERLEAVING_NOT_IMPLEMENTED);
		CloudSpannerXAConnection subject = createSubject();
		Xid xid = getRandomXid();
		subject.start(xid, XAResource.TMNOFLAGS);
		subject.end(xid, XAResource.TMSUCCESS);
		subject.start(getRandomXid(), XAResource.TMJOIN);
	}

	@Test
	public void testSuspend() throws SQLException, XAException
	{
		thrown.expect(CloudSpannerXAException.class);
		thrown.expectMessage(CloudSpannerXAException.SUSPEND_NOT_IMPLEMENTED);
		CloudSpannerXAConnection subject = createSubject();
		Xid xid = getRandomXid();
		subject.start(xid, XAResource.TMNOFLAGS);
		subject.end(xid, XAResource.TMSUSPEND);
	}

	@Test
	public void testEndWithoutStart() throws SQLException, XAException
	{
		thrown.expect(CloudSpannerXAException.class);
		thrown.expectMessage(CloudSpannerXAException.END_WITHOUT_START);
		CloudSpannerXAConnection subject = createSubject();
		subject.end(getRandomXid(), XAResource.TMSUCCESS);
	}

	@Test
	public void testPrepareWithoutEnd() throws SQLException, XAException
	{
		thrown.expect(CloudSpannerXAException.class);
		thrown.expectMessage(CloudSpannerXAException.PREPARE_BEFORE_END);
		CloudSpannerXAConnection subject = createSubject();
		Xid xid = getRandomXid();
		subject.start(xid, XAResource.TMNOFLAGS);
		subject.prepare(xid);
	}

	@Test
	public void testPrepareWithOtherXid() throws SQLException, XAException
	{
		thrown.expect(CloudSpannerXAException.class);
		thrown.expectMessage(CloudSpannerXAException.PREPARE_WITH_SAME);
		CloudSpannerXAConnection subject = createSubject();
		Xid xid = getRandomXid();
		subject.start(xid, XAResource.TMNOFLAGS);
		subject.end(xid, XAResource.TMSUCCESS);
		subject.prepare(getRandomXid());
	}

	private <T extends Throwable> void testExpectedError(RunnableWithException runnable, Class<T> expectedException)
	{
		testExpectedError(runnable, expectedException, null);
	}

	private <T extends Throwable> void testExpectedError(RunnableWithException runnable, Class<T> expectedException,
			String message)
	{
		try
		{
			runnable.run();
		}
		catch (Exception e)
		{
			Assert.assertEquals("Unexpected error: " + e.getMessage() + " - Expected " + expectedException.getName(),
					e.getClass(), expectedException);
			if (message != null)
			{
				Assert.assertTrue("Unexpected error message: " + e.getMessage() + " - Expected: " + message,
						e.getMessage().contains(message));
			}
		}
	}

	private Xid getRandomXid()
	{
		Random rnd = new Random();
		int id = rnd.nextInt();
		return RecoveredXid.stringToXid(String.valueOf(id) + "_Z3RyaWQ=_YnF1YWw=");
	}

}
