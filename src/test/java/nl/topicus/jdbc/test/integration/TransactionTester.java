package nl.topicus.jdbc.test.integration;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import nl.topicus.jdbc.CloudSpannerConnection;

public class TransactionTester
{

	private Connection connection;

	public TransactionTester(Connection connection)
	{
		this.connection = connection;
	}

	public void runTransactionTests() throws SQLException
	{
		connection.setAutoCommit(false);
		runRollbackTests();
		runReadOnlyTests();
		runAutoCommitTests();
		runRepeatableReadTest();
	}

	private void runAutoCommitTests() throws SQLException
	{
		connection.commit();
		connection.setAutoCommit(true);
		List<Object[]> originalRows = getResultList("SELECT * FROM TEST");
		insertRowInTest(100000l);
		List<Object[]> rows = getResultList("SELECT * FROM TEST");
		connection.setAutoCommit(false);
		Assert.assertEquals(originalRows.size() + 1, rows.size());
	}

	private void runReadOnlyTests() throws SQLException
	{
		boolean exception = false;
		connection.commit();
		connection.setReadOnly(true);
		List<Object[]> originalRows = getResultList("SELECT * FROM TEST");
		try
		{
			insertRowInTest(100000l);
			connection.commit();
		}
		catch (SQLException e)
		{
			connection.rollback();
			exception = true;
		}
		connection.setReadOnly(false);
		List<Object[]> rows = getResultList("SELECT * FROM TEST");
		Assert.assertTrue("Expected exception", exception);
		Assert.assertEquals(originalRows.size(), rows.size());
		for (int index = 0; index < rows.size(); index++)
			Assert.assertArrayEquals(originalRows.get(index), rows.get(index));
	}

	private void runRollbackTests() throws SQLException
	{
		List<Object[]> originalRows = getResultList("SELECT * FROM TEST");
		insertRowInTest(100000l);
		connection.rollback();
		List<Object[]> rows = getResultList("SELECT * FROM TEST");
		Assert.assertEquals(originalRows.size(), rows.size());
		for (int index = 0; index < rows.size(); index++)
			Assert.assertArrayEquals(originalRows.get(index), rows.get(index));
	}

	private void runRepeatableReadTest() throws SQLException
	{
		// Do a read on the original connection. This starts a transaction.
		List<Object[]> originalRows = getResultList("SELECT * FROM TEST");

		// Open a new connection and do an insert. This also starts a different
		// transaction.
		CloudSpannerConnection csConnection = (CloudSpannerConnection) connection;
		Connection otherConnection = DriverManager.getConnection(csConnection.getUrl(),
				csConnection.getSuppliedProperties());
		otherConnection.setAutoCommit(false);
		insertRowInTest(100001l);
		// Commit the changes
		otherConnection.commit();
		otherConnection.close();

		// Do a read on the original connection. This read should be equal to
		// the previous read.
		List<Object[]> rows = getResultList("SELECT * FROM TEST");
		Assert.assertEquals(originalRows.size(), rows.size());
		for (int index = 0; index < rows.size(); index++)
			Assert.assertArrayEquals(originalRows.get(index), rows.get(index));

		// Do a commit and then do the same query. Now the new row should be
		// found.
		connection.commit();
		List<Object[]> newRows = getResultList("SELECT * FROM TEST");
		Assert.assertEquals(originalRows.size() + 1, newRows.size());
	}

	private void insertRowInTest(long id) throws SQLException
	{
		PreparedStatement ps = connection.prepareStatement(
				"INSERT INTO TEST (ID, UUID, ACTIVE, AMOUNT, DESCRIPTION, CREATED_DATE, LAST_UPDATED) VALUES (?, ?, ?, ?, ?, ?, ?)");
		ps.setLong(1, id);
		ps.setBytes(2, "FOO".getBytes());
		ps.setBoolean(3, true);
		ps.setDouble(4, 50d);
		ps.setString(5, "BAR");
		ps.setDate(6, new Date(1000l));
		ps.setTimestamp(7, new Timestamp(5000l));
		ps.executeUpdate();
	}

	private List<Object[]> getResultList(String sql) throws SQLException
	{
		List<Object[]> res = new ArrayList<>();
		try (ResultSet rs = connection.prepareStatement(sql).executeQuery())
		{
			ResultSetMetaData metadata = rs.getMetaData();
			while (rs.next())
			{
				Object[] row = new Object[metadata.getColumnCount()];
				for (int i = 1; i <= metadata.getColumnCount(); i++)
				{
					row[i - 1] = rs.getObject(i);
				}
				res.add(row);
			}
		}
		return res;
	}
}
