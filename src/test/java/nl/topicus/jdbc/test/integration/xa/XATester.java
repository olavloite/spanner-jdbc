package nl.topicus.jdbc.test.integration.xa;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;
import java.util.logging.Logger;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.Assert;

import com.google.rpc.Code;

import nl.topicus.jdbc.CloudSpannerDriver;
import nl.topicus.jdbc.CloudSpannerXADataSource;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.xa.CloudSpannerXAConnection;
import nl.topicus.jdbc.xa.RecoveredXid;

public class XATester
{
	private static final Logger log = Logger.getLogger(XATester.class.getName());

	private enum CommitMode
	{
		None, OnePhase, TwoPhase;
	}

	public void testXA(String projectId, String instanceId, String database, String pvtKeyPath) throws SQLException
	{
		log.info("Starting XA tests");
		int originalLogLevel = CloudSpannerDriver.getLogLevel();
		CloudSpannerDriver.setLogLevel(CloudSpannerDriver.DEBUG);
		CloudSpannerXADataSource ds = new CloudSpannerXADataSource();
		ds.setProjectId(projectId);
		ds.setInstanceId(instanceId);
		ds.setDatabase(database);
		ds.setPvtKeyPath(pvtKeyPath);
		ds.setAllowExtendedMode(true);

		try (CloudSpannerXAConnection xaConnection = ds.getXAConnection())
		{
			testXATransaction(xaConnection, CommitMode.TwoPhase);
			testXARollback(xaConnection);
			deleteTestRow(xaConnection);
			testXARecover(xaConnection);
			deleteTestRow(xaConnection);
		}
		catch (Exception e)
		{
			throw new CloudSpannerSQLException("Exception occurred during XA tests", Code.INTERNAL, e);
		}
		finally
		{
			CloudSpannerDriver.setLogLevel(originalLogLevel);
		}
		log.info("Finished XA tests");
	}

	private void deleteTestRow(CloudSpannerXAConnection xaConnection) throws XAException, SQLException
	{
		Xid xid = prepareDeleteRow(xaConnection);
		xaConnection.commit(xid, false);
	}

	private void testXATransaction(CloudSpannerXAConnection xaConnection, CommitMode mode)
			throws SQLException, XAException
	{
		log.info("Starting XA simple transaction test");
		Connection connection = xaConnection.getConnection();
		Xid xid = getRandomXid();
		xaConnection.start(xid, XAResource.TMNOFLAGS);
		String sql = "insert into test (id, uuid, active, amount, description, created_date, last_updated) values (?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement statement = connection.prepareStatement(sql);
		setParameterValues(statement, 1000000);
		statement.executeUpdate();
		xaConnection.end(xid, XAResource.TMSUCCESS);
		xaConnection.prepare(xid);
		if (mode != CommitMode.None)
		{
			xaConnection.commit(xid, mode == CommitMode.OnePhase);
		}

		if (mode != CommitMode.None)
		{
			boolean found = false;
			try (ResultSet rs = connection.createStatement().executeQuery("select * from test where id=1000000"))
			{
				if (rs.next())
					found = true;
			}
			Assert.assertTrue(found);
		}
		log.info("Finished XA simple transaction test");
	}

	private void testXARollback(CloudSpannerXAConnection xaConnection) throws SQLException, XAException
	{
		log.info("Starting XA rollback transaction test");
		Xid xid = prepareDeleteRow(xaConnection);
		xaConnection.rollback(xid);

		boolean found = false;
		try (ResultSet rs = xaConnection.getConnection().createStatement()
				.executeQuery("select * from test where id=1000000"))
		{
			if (rs.next())
				found = true;
		}
		Assert.assertTrue(found);
		log.info("Finished XA rollback transaction test");
	}

	private Xid prepareDeleteRow(CloudSpannerXAConnection xaConnection) throws SQLException, XAException
	{
		Connection connection = xaConnection.getConnection();
		Xid xid = getRandomXid();
		xaConnection.start(xid, XAResource.TMNOFLAGS);
		String sql = "delete from test where id=1000000";
		PreparedStatement statement = connection.prepareStatement(sql);
		statement.executeUpdate();
		xaConnection.end(xid, XAResource.TMSUCCESS);
		xaConnection.prepare(xid);

		return xid;
	}

	private void testXARecover(CloudSpannerXAConnection xaConnection) throws SQLException, XAException
	{
		log.info("Started XA recover transaction test");
		testXATransaction(xaConnection, CommitMode.None);
		Xid[] xids = xaConnection.recover(XAResource.TMSTARTRSCAN);
		Assert.assertEquals(1, xids.length);
		xaConnection.commit(xids[0], false);
		boolean found = false;
		try (ResultSet rs = xaConnection.getConnection().createStatement()
				.executeQuery("select * from test where id=1000000"))
		{
			if (rs.next())
				found = true;
		}
		Assert.assertTrue(found);
		log.info("Finished XA recover transaction test");
	}

	private void setParameterValues(PreparedStatement statement, long id) throws SQLException
	{
		statement.setLong(1, id);
		statement.setBytes(2, new byte[] { 1, 2, 3 });
		statement.setBoolean(3, true);
		statement.setBigDecimal(4, BigDecimal.ONE);
		statement.setString(5, "xa test");
		statement.setDate(6, new Date(1000));
		statement.setTimestamp(7, new Timestamp(10000));
	}

	private Xid getRandomXid()
	{
		Random rnd = new Random();
		int id = rnd.nextInt();
		return RecoveredXid.stringToXid(String.valueOf(id) + "_Z3RyaWQ=_YnF1YWw=");
	}

}
