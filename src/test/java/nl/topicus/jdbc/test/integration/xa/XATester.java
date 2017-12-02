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

import nl.topicus.jdbc.CloudSpannerXADataSource;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.xa.CloudSpannerXAConnection;
import nl.topicus.jdbc.xa.RecoveredXid;

public class XATester
{
	private static final Logger log = Logger.getLogger(XATester.class.getName());

	public void testXA(String projectId, String instanceId, String database, String pvtKeyPath) throws SQLException
	{
		log.info("Starting XA tests");
		CloudSpannerXADataSource ds = new CloudSpannerXADataSource();
		ds.setProjectId(projectId);
		ds.setInstanceId(instanceId);
		ds.setDatabase(database);
		ds.setPvtKeyPath(pvtKeyPath);
		ds.setAllowExtendedMode(true);

		try (CloudSpannerXAConnection xaConnection = ds.getXAConnection())
		{
			testXATransaction(xaConnection);
			testXARollback(xaConnection);
		}
		catch (Exception e)
		{
			throw new CloudSpannerSQLException("Exception occurred during XA tests", Code.INTERNAL, e);
		}
		log.info("Finished XA tests");
	}

	private void testXATransaction(CloudSpannerXAConnection xaConnection) throws SQLException, XAException
	{
		log.info("Starting XA simple transaction test");
		Random rnd = new Random();
		Connection connection = xaConnection.getConnection();
		int id = rnd.nextInt();
		Xid xid = RecoveredXid.stringToXid(String.valueOf(id) + "_Z3RyaWQ=_YnF1YWw=");
		xaConnection.start(xid, XAResource.TMNOFLAGS);
		String sql = "insert into test (id, uuid, active, amount, description, created_date, last_updated) values (?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement statement = connection.prepareStatement(sql);
		setParameterValues(statement, 1000000);
		statement.executeUpdate();
		xaConnection.end(xid, XAResource.TMSUCCESS);
		xaConnection.prepare(xid);
		xaConnection.commit(xid, false);

		boolean found = false;
		try (ResultSet rs = connection.createStatement().executeQuery("select * from test where id=1000000"))
		{
			if (rs.next())
				found = true;
		}
		Assert.assertTrue(found);
		log.info("Finished XA simple transaction test");
	}

	private void testXARollback(CloudSpannerXAConnection xaConnection) throws SQLException, XAException
	{
		log.info("Starting XA rollback transaction test");
		Random rnd = new Random();
		Connection connection = xaConnection.getConnection();
		int id = rnd.nextInt();
		Xid xid = RecoveredXid.stringToXid(String.valueOf(id) + "_Z3RyaWQ=_YnF1YWw=");
		xaConnection.start(xid, XAResource.TMNOFLAGS);
		String sql = "delete from test where id=1000000";
		PreparedStatement statement = connection.prepareStatement(sql);
		statement.executeUpdate();
		xaConnection.end(xid, XAResource.TMSUCCESS);
		xaConnection.prepare(xid);
		xaConnection.rollback(xid);

		boolean found = false;
		try (ResultSet rs = connection.createStatement().executeQuery("select * from test where id=1000000"))
		{
			if (rs.next())
				found = true;
		}
		Assert.assertTrue(found);
		log.info("Finished XA rollback transaction test");
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

}
