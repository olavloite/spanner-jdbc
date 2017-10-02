package nl.topicus.jdbc.statement;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import com.google.api.client.util.Lists;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.cloud.spanner.Value;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDatabaseMetaData;
import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;
import nl.topicus.jdbc.resultset.CloudSpannerResultSet;
import nl.topicus.jdbc.test.category.UnitTest;

@RunWith(Enclosed.class)
@Category(UnitTest.class)
public class CloudSpannerPreparedStatementTest
{

	public static class DeleteStatementTests
	{
		@Rule
		public ExpectedException thrown = ExpectedException.none();

		@Test
		public void testDeleteStatementWithoutWhereClause() throws SQLException
		{
			Mutation deleteMutation = getMutation("DELETE FROM FOO");
			Assert.assertNotNull(deleteMutation);
			Assert.assertEquals(Op.DELETE, deleteMutation.getOperation());
			Assert.assertTrue(deleteMutation.getKeySet().isAll());
			Assert.assertNotNull(deleteMutation);
			Assert.assertEquals(Op.DELETE, deleteMutation.getOperation());
			Assert.assertTrue(deleteMutation.getKeySet().isAll());
		}

		@Test
		public void testDeleteStatementWithWhereClause() throws SQLException
		{
			Mutation deleteMutation = getMutation("DELETE FROM FOO WHERE ID=1");
			Assert.assertNotNull(deleteMutation);
			Assert.assertEquals(Op.DELETE, deleteMutation.getOperation());
			List<Key> keys = Lists.newArrayList(deleteMutation.getKeySet().getKeys());
			Assert.assertEquals(1, keys.size());
			Assert.assertEquals(1l, keys.get(0).getParts().iterator().next());
		}

		@Test
		public void testDeleteStatementWithInWhereClauses() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID IN (1,2)");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID IN (1, 2)",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithoutIdInWhere() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE BAR=1");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE BAR = 1", mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithNullValue() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID IS NULL");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID IS NULL",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithFunction() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE TO_STRING(ID)=1");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE TO_STRING(ID) = 1",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithNamedParameter() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=:NAMED_PARAMETER");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = :NAMED_PARAMETER",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithAddition() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=1+2");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = 1 + 2",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithDivision() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=1/2");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = 1 / 2",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithMultiplication() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=1*2");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = 1 * 2",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithSubtraction() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=1-2");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = 1 - 2",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithOr() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=1 OR ID=2");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = 1 OR ID = 2",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithBetween() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID BETWEEN 1 AND 10");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID BETWEEN 1 AND 10",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithLargerThan() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID>2");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID > 2", mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithLargerOrEquals() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID>=2");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID >= 2", mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithLessThan() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID<2");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID < 2", mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithLessOrEquals() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID<=2");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID <= 2", mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithLike() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID LIKE 'TEST'");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID LIKE 'TEST'",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithNotEquals() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID!=1");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID != 1", mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithSubSelect() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=(SELECT 1 FROM BAR)");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = (SELECT 1 FROM BAR)",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithCaseStatement() throws SQLException
		{
			Mutations mutations = getMutations(
					"DELETE FROM FOO WHERE ID=CASE WHEN FOO=1 THEN 1 WHEN FOO=2 THEN 2 ELSE 3 END");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals(
					"SELECT `FOO`.`ID` FROM `FOO` WHERE ID = CASE WHEN FOO = 1 THEN 1 WHEN FOO = 2 THEN 2 ELSE 3 END",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithExists() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE EXISTS (SELECT ID FROM FOO WHERE ID=1)");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE EXISTS (SELECT ID FROM FOO WHERE ID = 1)",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithAll() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=ALL (SELECT ID FROM FOO WHERE ID=1)");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = ALL (SELECT ID FROM FOO WHERE ID = 1)",
					mutations.getWorker().select.toString());
		}
	}

	public static class UpdateStatementTests
	{
		@Rule
		public ExpectedException thrown = ExpectedException.none();

		@Test
		public void testUpdateStatementWithoutWhereClause() throws SQLException
		{
			Mutations mutations = getMutations("UPDATE FOO SET COL1=1, COL2=2");
			Assert.assertTrue(mutations.isWorker());
			Assert.assertNotNull(mutations.getWorker());
			Assert.assertEquals(InsertWorker.class, mutations.getWorker().getClass());
		}

		@Test
		public void testUpdateStatementWithWhereClause() throws SQLException
		{
			Mutation updateMutation = getMutation("UPDATE FOO SET COL1=1, COL2=2 WHERE ID=1");
			Assert.assertNotNull(updateMutation);
			Assert.assertEquals(Op.UPDATE, updateMutation.getOperation());
			Assert.assertEquals("FOO", updateMutation.getTable());
			List<String> columns = Lists.newArrayList(updateMutation.getColumns());
			Assert.assertArrayEquals(new String[] { "COL1", "COL2", "ID" }, columns.toArray());
			Assert.assertArrayEquals(new String[] { "1", "2", "1" }, getValues(updateMutation.getValues()));
		}

		@Test
		public void testUpdateStatementWithMultipleWhereClauses() throws SQLException
		{
			Mutation updateMutation = getMutation("UPDATE FOO SET COL1=1, COL2=2 WHERE ID1=1 AND ID2=1");
			Assert.assertNotNull(updateMutation);
			Assert.assertEquals(Op.UPDATE, updateMutation.getOperation());
			Assert.assertEquals("FOO", updateMutation.getTable());
			List<String> columns = Lists.newArrayList(updateMutation.getColumns());
			Assert.assertArrayEquals(new String[] { "COL1", "COL2", "ID1", "ID2" }, columns.toArray());
			Assert.assertArrayEquals(new String[] { "1", "2", "1", "1" }, getValues(updateMutation.getValues()));
		}
	}

	public static class InsertStatementTests
	{
		@Rule
		public ExpectedException thrown = ExpectedException.none();

		@Test
		public void testInsertStatement() throws SQLException
		{
			assertSingleInsert(getMutation("INSERT INTO FOO (COL1, COL2, COL3) VALUES (1, 'two', 0xaa)"),
					Mutation.Op.INSERT);
		}

		@Test
		public void testInsertOnDuplicateKeyStatementWithDiffentUpdateValues() throws SQLException
		{
			assertSingleInsert(
					getMutation(
							"INSERT INTO FOO (COL1, COL2, COL3) VALUES (1, 'two', 0xaa) ON DUPLICATE KEY UPDATE COL2='three', COL3=0xbb"),
					Mutation.Op.INSERT_OR_UPDATE);
		}

		@Test
		public void testInsertOnDuplicateKeyStatementWithEqualUpdateValues() throws SQLException
		{
			assertSingleInsert(
					getMutation(
							"INSERT INTO FOO (COL1, COL2, COL3) VALUES (1, 'two', 0xaa) ON DUPLICATE KEY UPDATE COL2='two', COL3=0xaa"),
					Mutation.Op.INSERT_OR_UPDATE);
		}

		@Test
		public void assertInsertWithSelect() throws SQLException
		{
			Mutations mutations = getMutations("INSERT INTO FOO (COL1, COL2, COL3) SELECT 1, 'two', 0xaa");
			Assert.assertTrue(mutations.isWorker());
			Assert.assertNotNull(mutations.getWorker());
			Assert.assertEquals(InsertWorker.class, mutations.getWorker().getClass());
		}

		@Test
		public void assertInsertWithSelectTable() throws SQLException
		{
			Mutations mutations = getMutations(
					"INSERT INTO FOO (COL1, COL2, COL3) SELECT COLA, COLB, COLC FROM OTHER_TABLE");
			Assert.assertTrue(mutations.isWorker());
			Assert.assertNotNull(mutations.getWorker());
			Assert.assertEquals(InsertWorker.class, mutations.getWorker().getClass());
		}

		private void assertSingleInsert(Mutation mutation, Mutation.Op operation)
		{
			Assert.assertNotNull(mutation);
			Assert.assertEquals(operation, mutation.getOperation());
			Assert.assertEquals("FOO", mutation.getTable());
			List<String> columns = Lists.newArrayList(mutation.getColumns());
			Assert.assertArrayEquals(new String[] { "COL1", "COL2", "COL3" }, columns.toArray());
			Assert.assertArrayEquals(
					new String[] { "1", "two", ByteArray.copyFrom(DatatypeConverter.parseHexBinary("aa")).toString() },
					getValues(mutation.getValues()));
		}
	}

	public static class DDLStatementTests
	{
		@Rule
		public ExpectedException thrown = ExpectedException.none();

		@Test
		public void testCreateTableStatement() throws SQLException
		{
			CloudSpannerPreparedStatementTest
					.testCreateTableStatement("CREATE TABLE FOO (ID INT64, NAME STRING(100)) PRIMARY KEY (ID)");
		}

		/**
		 * TODO: quote the identifier in primary key clause when this is fixed
		 * in the underlying SQL Parser
		 * 
		 * @throws SQLException
		 */
		@Test
		public void testQuotedCreateTableStatement() throws SQLException
		{
			CloudSpannerPreparedStatementTest
					.testCreateTableStatement("CREATE TABLE `FOO` (`ID` INT64, `NAME` STRING(100)) PRIMARY KEY (ID)");
		}
	}

	private static void testCreateTableStatement(String sql) throws SQLException
	{
		boolean isDDL = isDDLStatement(sql);
		Assert.assertTrue(isDDL);
		Statement statement = null;
		try
		{
			statement = CCJSqlParserUtil.parse(sql);
		}
		catch (JSQLParserException e)
		{
			throw new SQLException("Could not parse SQL statement", e);
		}
		Assert.assertNotNull(statement);
		Assert.assertEquals(CreateTable.class, statement.getClass());
	}

	private static String[] getValues(Iterable<Value> values)
	{
		List<Value> valueList = Lists.newArrayList(values);
		String[] res = new String[valueList.size()];
		int index = 0;
		for (Value value : valueList)
		{
			res[index] = value.toString();
			index++;
		}
		return res;
	}

	private static boolean isDDLStatement(String sql) throws SQLException
	{
		boolean res = false;
		CloudSpannerPreparedStatement ps = new CloudSpannerPreparedStatement(sql, null, null);
		try
		{
			Method isDDLStatement = CloudSpannerStatement.class.getDeclaredMethod("isDDLStatement", String.class);
			isDDLStatement.setAccessible(true);
			res = (boolean) isDDLStatement.invoke(ps, sql);
		}
		catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException e)
		{
			throw new RuntimeException(e);
		}
		catch (InvocationTargetException e)
		{
			if (e.getTargetException() instanceof SQLException)
			{
				throw (SQLException) e.getTargetException();
			}
			throw new RuntimeException(e);
		}
		return res;
	}

	private static Mutation getMutation(String sql) throws SQLException
	{
		return getMutations(sql).getMutations().get(0);
	}

	public static Mutations getMutations(String sql) throws SQLException
	{
		Mutations mutations = null;
		CloudSpannerConnection connection = Mockito.mock(CloudSpannerConnection.class);
		CloudSpannerDatabaseMetaData metadata = Mockito.mock(CloudSpannerDatabaseMetaData.class);
		CloudSpannerResultSet resultSet = Mockito.mock(CloudSpannerResultSet.class);
		TableKeyMetaData table = Mockito.mock(TableKeyMetaData.class);
		Mockito.when(connection.getMetaData()).thenReturn(metadata);
		Mockito.when(metadata.getPrimaryKeys(null, null, "FOO")).thenReturn(resultSet);
		Mockito.when(resultSet.next()).thenReturn(true, false);
		Mockito.when(resultSet.getString("COLUMN_NAME")).thenReturn("ID");
		Mockito.when(connection.getTable("FOO")).thenReturn(table);
		Mockito.when(table.getKeyColumns()).thenReturn(Arrays.asList("ID"));
		CloudSpannerPreparedStatement ps = new CloudSpannerPreparedStatement(sql, connection, null);
		try
		{
			Method createMutations = ps.getClass().getDeclaredMethod("createMutations", String.class);
			createMutations.setAccessible(true);
			mutations = (Mutations) createMutations.invoke(ps, sql);
		}
		catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException e)
		{
			throw new RuntimeException(e);
		}
		catch (InvocationTargetException e)
		{
			if (e.getTargetException() instanceof SQLException)
			{
				throw (SQLException) e.getTargetException();
			}
			throw new RuntimeException(e);
		}
		return mutations;
	}

}
