package nl.topicus.jdbc.xa;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.Random;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

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
}
