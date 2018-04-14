package nl.topicus.jdbc.resultset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.ResultSet;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.statement.CloudSpannerStatement;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerPartitionResultSetTest
{
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private CloudSpannerPartitionResultSet createSubject()
	{
		Partition partition = mock(Partition.class);
		BatchReadOnlyTransaction transaction = mock(BatchReadOnlyTransaction.class);
		ResultSet rs = CloudSpannerResultSetTest.getMockResultSet();
		when(transaction.execute(partition)).thenReturn(rs);
		return new CloudSpannerPartitionResultSet(mock(CloudSpannerStatement.class), transaction, partition,
				"SELECT * FROM FOO");
	}

	@Test
	public void testNext() throws SQLException
	{
		try (CloudSpannerPartitionResultSet rs = createSubject())
		{
			assertTrue(rs.isBeforeFirst());
			assertEquals(false, rs.isAfterLast());
			int num = 0;
			while (rs.next())
			{
				num++;
			}
			assertEquals(4, num);
			assertEquals(false, rs.isBeforeFirst());
			assertTrue(rs.isAfterLast());
		}
	}

	@Test
	public void testGetMetaData() throws SQLException
	{
		try (CloudSpannerPartitionResultSet rs = createSubject())
		{
			CloudSpannerResultSetMetaData metadata = rs.getMetaData();
			assertNotNull(metadata);
		}
	}

	@Test
	public void testFindColumn() throws SQLException
	{
		try (CloudSpannerPartitionResultSet rs = createSubject())
		{
			assertEquals(2, rs.findColumn(CloudSpannerResultSetTest.STRING_COL_NOT_NULL));
		}
	}

	@Test
	public void testGetWithoutNext() throws SQLException
	{
		try (CloudSpannerPartitionResultSet rs = createSubject())
		{
			thrown.expect(CloudSpannerSQLException.class);
			thrown.expectMessage("Before first record");
			rs.getBigDecimal(CloudSpannerResultSetTest.DOUBLE_COL_NOT_NULL);
		}
	}

	@Test
	public void testGetWithNext() throws SQLException
	{
		try (CloudSpannerPartitionResultSet rs = createSubject())
		{
			while (rs.next())
			{
				assertNotNull(rs.getBigDecimal(CloudSpannerResultSetTest.DOUBLE_COL_NOT_NULL));
			}
		}
	}

}
