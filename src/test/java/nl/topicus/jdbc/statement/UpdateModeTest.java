package nl.topicus.jdbc.statement;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.resultset.CloudSpannerResultSet;
import nl.topicus.jdbc.resultset.CloudSpannerResultSetMetaData;
import nl.topicus.jdbc.statement.AbstractTablePartWorker.DMLOperation;
import nl.topicus.jdbc.test.category.UnitTest;

@RunWith(Enclosed.class)
@Category(UnitTest.class)
public class UpdateModeTest
{

	public static class InsertTests
	{

		@Test
		public void assertInsertWithSelect() throws SQLException
		{
			Mutations mutations = CloudSpannerPreparedStatementTest
					.getMutations("INSERT INTO FOO (COL1, COL2, COL3) SELECT 1, 'two', 0xaa");
			InsertWorker worker = (InsertWorker) mutations.getWorker();
			Assert.assertEquals("SELECT 1, 'two', 0xaa", worker.select.toString());
		}

		@Test
		public void assertInsertWithSelectTable() throws SQLException
		{
			Mutations mutations = CloudSpannerPreparedStatementTest
					.getMutations("INSERT INTO FOO (COL1, COL2, COL3) SELECT COLA, COLB, COLC FROM OTHER_TABLE");
			InsertWorker worker = (InsertWorker) mutations.getWorker();
			Assert.assertEquals("SELECT COLA, COLB, COLC FROM OTHER_TABLE", worker.select.toString());
		}
	}

	public static class UpdateTests
	{
		@Test
		public void testUpdateStatementWithoutWhereClause() throws SQLException
		{
			Mutations mutations = CloudSpannerPreparedStatementTest.getMutations("UPDATE FOO SET COL1=1, COL2=2");
			InsertWorker worker = (InsertWorker) mutations.getWorker();
			Assert.assertEquals("SELECT `FOO`.`ID`, 1, 2 FROM `FOO`", worker.select.toString());
			Assert.assertEquals(DMLOperation.UPDATE, worker.operation);
			Assert.assertEquals(
					"INSERT INTO `FOO` (`ID`, `COL1`, `COL2`) SELECT `FOO`.`ID`, 1, 2 FROM `FOO` ON DUPLICATE KEY UPDATE FOO = BAR",
					worker.insert.toString());

			mockConnection(worker.connection);
			ConversionResult res = worker.call();
			Assert.assertNotNull(res);
			Assert.assertNull(res.getException());
			Assert.assertEquals(2l, res.getRecordCount());
		}

		@Test
		public void testUpdateStatementWithWhereClause() throws SQLException
		{
			Mutations mutations = CloudSpannerPreparedStatementTest
					.getMutations("UPDATE FOO SET COL1=COL1+COL2, COL2=COL2*1.1 WHERE COL1<100");
			InsertWorker worker = (InsertWorker) mutations.getWorker();
			Assert.assertEquals("SELECT `FOO`.`ID`, COL1 + COL2, COL2 * 1.1 FROM `FOO` WHERE COL1 < 100",
					worker.select.toString());
			Assert.assertEquals(DMLOperation.UPDATE, worker.operation);
			Assert.assertEquals(
					"INSERT INTO `FOO` (`ID`, `COL1`, `COL2`) SELECT `FOO`.`ID`, COL1 + COL2, COL2 * 1.1 FROM `FOO` WHERE COL1 < 100 ON DUPLICATE KEY UPDATE FOO = BAR",
					worker.insert.toString());

			mockConnection(worker.connection);
			ConversionResult res = worker.call();
			Assert.assertNotNull(res);
			Assert.assertNull(res.getException());
			Assert.assertEquals(2l, res.getRecordCount());
		}
	}

	private static void mockConnection(CloudSpannerConnection connection) throws SQLException
	{
		CloudSpannerPreparedStatement insertStatement = Mockito.mock(CloudSpannerPreparedStatement.class);
		CloudSpannerPreparedStatement selectStatement = Mockito.mock(CloudSpannerPreparedStatement.class);
		CloudSpannerPreparedStatement countStatement = Mockito.mock(CloudSpannerPreparedStatement.class);
		CloudSpannerResultSet selectResult = Mockito.mock(CloudSpannerResultSet.class);
		CloudSpannerResultSet countResult = Mockito.mock(CloudSpannerResultSet.class);
		CloudSpannerResultSetMetaData selectMetadata = Mockito.mock(CloudSpannerResultSetMetaData.class);
		Mockito.when(selectMetadata.getColumnCount()).thenReturn(3);
		Mockito.when(connection.prepareStatement(Mockito.startsWith("INSERT"))).thenReturn(insertStatement);
		Mockito.when(connection.prepareStatement(Mockito.startsWith("SELECT `FOO`"))).thenReturn(selectStatement);
		Mockito.when(connection.prepareStatement(Mockito.startsWith("SELECT COUNT(*)"))).thenReturn(countStatement);
		Mockito.when(selectStatement.executeQuery()).thenReturn(selectResult);
		Mockito.when(selectResult.next()).thenReturn(true, true, false);
		Mockito.when(selectResult.getObject(1)).thenReturn(1L, 2L);
		Mockito.when(selectResult.getObject(2)).thenReturn("One", "Two");
		Mockito.when(selectResult.getObject(3)).thenReturn("En", "To");
		Mockito.when(selectResult.getMetaData()).thenReturn(selectMetadata);
		Mockito.when(countStatement.executeQuery()).thenReturn(countResult);
		Mockito.when(countResult.next()).thenReturn(true, false);
		Mockito.when(countResult.getLong(1)).thenReturn(2L);
		Mockito.when(insertStatement.executeUpdate()).thenReturn(1, 1);
	}

}
