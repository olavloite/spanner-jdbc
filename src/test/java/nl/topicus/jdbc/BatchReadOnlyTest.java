package nl.topicus.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.internal.stubbing.answers.Returns;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.TimestampBound;

import nl.topicus.jdbc.test.category.UnitTest;
import nl.topicus.jdbc.transaction.CloudSpannerTransaction;

@Category(UnitTest.class)
public class BatchReadOnlyTest
{
	private static final String SELECT_ALL_FROM_FOO = "SELECT * FROM FOO";

	private CloudSpannerConnection connection;

	@Before
	public void setup() throws SQLException
	{
		connection = new CloudSpannerConnection();
		connection.setAutoCommit(false);
	}

	@Test
	public void testSetBatchReadOnlyMode() throws SQLException
	{
		assertFalse(connection.isBatchReadOnly());
		connection.setBatchReadOnly(true);
		assertTrue(connection.isBatchReadOnly());
	}

	@Test
	public void testExecuteBatchReadOnly() throws SQLException, NoSuchFieldException, SecurityException,
			IllegalArgumentException, IllegalAccessException
	{
		for (int testRun = 0; testRun < 2; testRun++)
		{
			final int numberOfPartitions = 6;
			BatchClient batchClient = mock(BatchClient.class);
			BatchReadOnlyTransaction tx = mock(BatchReadOnlyTransaction.class);
			List<Partition> partitions = new ArrayList<>(numberOfPartitions);
			for (int i = 0; i < numberOfPartitions; i++)
				partitions.add(mock(Partition.class));
			when(tx.partitionQuery(any(), any())).then(new Returns(partitions));
			when(batchClient.batchReadOnlyTransaction(TimestampBound.strong())).then(new Returns(tx));
			Field field = CloudSpannerTransaction.class.getDeclaredField("batchClient");
			field.setAccessible(true);
			field.set(connection.getTransaction(), batchClient);
			connection.setBatchReadOnly(true);
			Statement statement;
			if (testRun % 2 == 0)
			{
				statement = connection.createStatement();
				assertTrue(statement.execute(SELECT_ALL_FROM_FOO));
			}
			else
			{
				PreparedStatement ps = connection.prepareStatement(SELECT_ALL_FROM_FOO);
				assertTrue(ps.execute());
				statement = ps;
			}
			List<ResultSet> resultSets = new ArrayList<>();
			do
			{
				resultSets.add(statement.getResultSet());
			}
			while (statement.getMoreResults());
			assertEquals(numberOfPartitions, resultSets.size());
		}
	}

	@Test
	public void testExecuteNormal() throws SQLException, NoSuchFieldException, SecurityException,
			IllegalArgumentException, IllegalAccessException
	{
		for (int testRun = 0; testRun < 2; testRun++)
		{
			CloudSpannerTransaction tx = mock(CloudSpannerTransaction.class);
			com.google.cloud.spanner.ResultSet rs = mock(com.google.cloud.spanner.ResultSet.class);
			when(tx.executeQuery(any())).then(new Returns(rs));
			Field field = CloudSpannerConnection.class.getDeclaredField("transaction");
			field.setAccessible(true);
			field.set(connection, tx);
			Statement statement;
			if (testRun % 2 == 0)
			{
				statement = connection.createStatement();
				assertTrue(statement.execute(SELECT_ALL_FROM_FOO));
			}
			else
			{
				PreparedStatement ps = connection.prepareStatement(SELECT_ALL_FROM_FOO);
				assertTrue(ps.execute());
				statement = ps;
			}
			List<ResultSet> resultSets = new ArrayList<>();
			do
			{
				resultSets.add(statement.getResultSet());
			}
			while (statement.getMoreResults());
			assertEquals(1, resultSets.size());
			connection.commit();
		}
	}

}
