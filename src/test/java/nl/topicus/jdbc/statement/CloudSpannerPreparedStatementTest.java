package nl.topicus.jdbc.statement;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

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
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.cloud.spanner.Value;
import com.google.rpc.Code;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import nl.topicus.jdbc.CloudSpannerArray;
import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.jdbc.test.category.UnitTest;
import nl.topicus.jdbc.test.util.CloudSpannerTestObjects;

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

		@Test()
		public void testDeleteStatementWithAny() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=ANY (SELECT ID FROM FOO WHERE ID=1)");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = ANY (SELECT ID FROM FOO WHERE ID = 1)",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithConcat() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=CONCAT(ID, COL1)");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = CONCAT(ID, COL1)",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithBitwiseAnd() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=1 & 2");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = 1 & 2",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithBitwiseOr() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=1 | 2");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = 1 | 2",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithBitwiseXOr() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=1 ^ 2");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = 1 ^ 2",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithCast() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=CAST('123' AS INT64)");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = CAST('123' AS INT64)",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithModulo() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=1 % 2");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = 1 % 2",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithExtract() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=EXTRACT(DAY FROM COL1)");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = EXTRACT(DAY FROM COL1)",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithInterval() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=900 * INTERVAL '1 SECOND'");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = 900 * INTERVAL '1 SECOND'",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithRegExpMatch() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID~'T[EA]ST'");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID ~ 'T[EA]ST'",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithNot() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE NOT ID=1");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE NOT ID = 1",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithRegExp() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID REGEXP 'T[EA]ST'");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID REGEXP 'T[EA]ST'",
					mutations.getWorker().select.toString());
		}

		@Test()
		public void testDeleteStatementWithUserVariable() throws SQLException
		{
			Mutations mutations = getMutations("DELETE FROM FOO WHERE ID=@FOO");
			Assert.assertEquals(DeleteWorker.class, mutations.getWorker().getClass());
			Assert.assertEquals("SELECT `FOO`.`ID` FROM `FOO` WHERE ID = @FOO",
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
		public void testUpdateStatementWithNullValue() throws SQLException
		{
			Mutations mutations = getMutations("UPDATE FOO SET COL1=1, COL2=2 WHERE COL1=NULL");
			Assert.assertTrue(mutations.isWorker());
			Assert.assertNotNull(mutations.getWorker());
			Assert.assertEquals(InsertWorker.class, mutations.getWorker().getClass());
		}

		@Test
		public void testUpdateStatementWithConcat() throws SQLException
		{
			Mutations mutations = getMutations("UPDATE FOO SET COL1=1, COL2=2 WHERE COL1='FOO' || 'BAR'");
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
			Mutation updateMutation = getMutation("UPDATE BAR SET COL1=1, COL2=2 WHERE ID1=1 AND ID2=1");
			Assert.assertNotNull(updateMutation);
			Assert.assertEquals(Op.UPDATE, updateMutation.getOperation());
			Assert.assertEquals("BAR", updateMutation.getTable());
			List<String> columns = Lists.newArrayList(updateMutation.getColumns());
			Assert.assertArrayEquals(new String[] { "COL1", "COL2", "ID1", "ID2" }, columns.toArray());
			Assert.assertArrayEquals(new String[] { "1", "2", "1", "1" }, getValues(updateMutation.getValues()));
		}

		@Test
		public void testUpdateStatementWithIDInUpdate() throws SQLException
		{
			thrown.expect(CloudSpannerSQLException.class);
			thrown.expectMessage("UPDATE of a primary key value is not allowed, cannot UPDATE the column(s) ");
			getMutation("UPDATE FOO SET COL1=1, ID=2 WHERE COL2=5");
		}

		@Test
		public void testUpdateStatementWithComment() throws SQLException
		{
			for (String sql : new String[] { "/* UPDATE TWO COLUMNS*/\nUPDATE FOO SET COL1=1, COL2=2",
					"--SINGLE LINE COMMENT\nUPDATE FOO SET COL1=1, COL2=2" })
			{
				Mutations mutations = getMutations(sql);
				Assert.assertTrue(mutations.isWorker());
				Assert.assertNotNull(mutations.getWorker());
				Assert.assertEquals(InsertWorker.class, mutations.getWorker().getClass());
			}
		}

		@Test
		public void testUpdateStatementWithQuotedColumns() throws SQLException
		{
			String sql = "UPDATE `FOO` SET `COL1` = ? WHERE `ID` > ?";
			Mutations mutations = getMutations(sql);
			Assert.assertTrue(mutations.isWorker());
			Assert.assertNotNull(mutations.getWorker());
			Assert.assertEquals(InsertWorker.class, mutations.getWorker().getClass());
		}

		private static final String TEST_SQL = "UPDATE FOO SET COL1=1, COL2=2";

		@Test
		public void testExecuteSql() throws SQLException
		{
			prepareUnsupportedStatementTest(TEST_SQL).execute(TEST_SQL);
		}

		@Test
		public void testExecuteSqlInt() throws SQLException
		{
			prepareUnsupportedStatementTest(TEST_SQL).execute(TEST_SQL, 0);
		}

		@Test
		public void testExecuteSqlIntArray() throws SQLException
		{
			prepareUnsupportedStatementTest(TEST_SQL).execute(TEST_SQL, new int[3]);
		}

		@Test
		public void testExecuteSqlStringArray() throws SQLException
		{
			prepareUnsupportedStatementTest(TEST_SQL).execute(TEST_SQL, new String[3]);
		}

		@Test
		public void testExecuteUpdateSql() throws SQLException
		{
			prepareUnsupportedStatementTest(TEST_SQL).executeUpdate(TEST_SQL);
		}

		@Test
		public void testExecuteUpdateSqlInt() throws SQLException
		{
			prepareUnsupportedStatementTest(TEST_SQL).executeUpdate(TEST_SQL, 0);
		}

		@Test
		public void testExecuteUpdateSqlIntArray() throws SQLException
		{
			prepareUnsupportedStatementTest(TEST_SQL).executeUpdate(TEST_SQL, new int[3]);
		}

		@Test
		public void testExecuteUpdateSqlStringArray() throws SQLException
		{
			prepareUnsupportedStatementTest(TEST_SQL).executeUpdate(TEST_SQL, new String[3]);
		}

		private CloudSpannerPreparedStatement prepareUnsupportedStatementTest(String sql) throws SQLException
		{
			thrown.expect(CloudSpannerSQLException.class);
			thrown.expectMessage("may not be called on a PreparedStatement");
			CloudSpannerConnection connection = Mockito.mock(CloudSpannerConnection.class, CALLS_REAL_METHODS);
			CloudSpannerPreparedStatement statement = connection.prepareStatement(sql);

			return statement;
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
		public void testInsertStatementWithoutColumns() throws SQLException
		{
			thrown.expect(SQLException.class);
			thrown.expectMessage("Insert statement must specify a list of column names");
			assertSingleInsert(getMutation("INSERT INTO FOO VALUES (1, 'two', 0xaa)"), Mutation.Op.INSERT);
		}

		@Test
		public void testSingleInsertStatementOnReadOnlyConnection() throws SQLException
		{
			thrown.expect(CloudSpannerSQLException.class);
			thrown.expectMessage(AbstractCloudSpannerStatement.NO_MUTATIONS_IN_READ_ONLY_MODE_EXCEPTION);
			String sql = "INSERT INTO FOO (ID, COL1, COL2) VALUES (?, ?, ?)";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			ps.getConnection().setReadOnly(true);
			ps.executeUpdate();
		}

		@Test
		public void testBulkInsertStatementOnReadOnlyConnection() throws SQLException
		{
			thrown.expect(CloudSpannerSQLException.class);
			thrown.expectMessage(AbstractCloudSpannerStatement.NO_MUTATIONS_IN_READ_ONLY_MODE_EXCEPTION);
			String sql = "INSERT INTO FOO (ID, COL1, COL2) SELECT 1, 2, 3 FROM FOO";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			ps.getConnection().setReadOnly(true);
			ps.executeUpdate();
		}

		@Test
		public void testInsertOnDuplicateKeyStatementWithDiffentUpdateValues() throws SQLException
		{
			assertSingleInsert(getMutation(
					"INSERT INTO FOO (COL1, COL2, COL3) VALUES (1, 'two', 0xaa) ON DUPLICATE KEY UPDATE COL2='three', COL3=0xbb"),
					Mutation.Op.INSERT_OR_UPDATE);
		}

		@Test
		public void testInsertOnDuplicateKeyStatementWithEqualUpdateValues() throws SQLException
		{
			assertSingleInsert(getMutation(
					"INSERT INTO FOO (COL1, COL2, COL3) VALUES (1, 'two', 0xaa) ON DUPLICATE KEY UPDATE COL2='two', COL3=0xaa"),
					Mutation.Op.INSERT_OR_UPDATE);
		}

		@Test
		public void testInsertWithSelect() throws SQLException
		{
			Mutations mutations = getMutations("INSERT INTO FOO (COL1, COL2, COL3) SELECT 1, 'two', 0xaa");
			Assert.assertTrue(mutations.isWorker());
			Assert.assertNotNull(mutations.getWorker());
			Assert.assertEquals(InsertWorker.class, mutations.getWorker().getClass());
		}

		@Test
		public void testInsertWithSelectTable() throws SQLException
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

	public static class BatchTests
	{
		@Rule
		public ExpectedException thrown = ExpectedException.none();

		@Test
		public void testBatchedInsertStatements() throws SQLException, NoSuchFieldException, SecurityException,
				IllegalArgumentException, IllegalAccessException
		{
			Field batchMutationsField = CloudSpannerPreparedStatement.class.getDeclaredField("batchMutations");
			batchMutationsField.setAccessible(true);
			String sql = "INSERT INTO FOO (COL1, COL2, COL3) VALUES (?, ?, ?)";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			for (int i = 1; i <= 2; i++)
			{
				ps.setInt(1, i);
				ps.setString(2, String.valueOf(i));
				ps.setBytes(3, String.valueOf(i).getBytes());
				ps.addBatch();
				@SuppressWarnings("unchecked")
				List<Mutations> batchMutations = (List<Mutations>) batchMutationsField.get(ps);
				Assert.assertEquals(i, batchMutations.size());
			}
			int[] res = ps.executeBatch();
			Assert.assertArrayEquals(new int[] { 1, 1 }, res);
			@SuppressWarnings("unchecked")
			List<Mutations> batchMutations = (List<Mutations>) batchMutationsField.get(ps);
			Assert.assertEquals(0, batchMutations.size());
		}

		@Test
		public void testClearBatch() throws SQLException, NoSuchFieldException, SecurityException,
				IllegalArgumentException, IllegalAccessException
		{
			Field batchMutationsField = CloudSpannerPreparedStatement.class.getDeclaredField("batchMutations");
			batchMutationsField.setAccessible(true);
			String sql = "INSERT INTO FOO (COL1, COL2, COL3) VALUES (?, ?, ?)";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			for (int i = 1; i <= 2; i++)
			{
				ps.setInt(1, i);
				ps.setString(2, String.valueOf(i));
				ps.setBytes(3, String.valueOf(i).getBytes());
				ps.addBatch();
				@SuppressWarnings("unchecked")
				List<Mutations> batchMutations = (List<Mutations>) batchMutationsField.get(ps);
				Assert.assertEquals(i, batchMutations.size());
			}
			ps.clearBatch();
			@SuppressWarnings("unchecked")
			List<Mutations> batchMutations = (List<Mutations>) batchMutationsField.get(ps);
			Assert.assertEquals(0, batchMutations.size());
			int[] res = ps.executeBatch();
			Assert.assertArrayEquals(new int[] {}, res);
		}

		@Test
		public void testBatchWithSelect() throws SQLException, NoSuchFieldException, SecurityException,
				IllegalArgumentException, IllegalAccessException
		{
			String sql = "\nSELECT * FROM FOO WHERE ID<?";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			ps.setLong(1, 1000l);
			thrown.expect(SQLFeatureNotSupportedException.class);
			thrown.expectMessage("SELECT statements may not be batched");
			ps.addBatch();
		}

		@Test
		public void testBatchWithDDL() throws SQLException, NoSuchFieldException, SecurityException,
				IllegalArgumentException, IllegalAccessException
		{
			String sql = "\nCREATE TABLE FOO (ID INT64 NOT NULL, COL1 STRING(100)) PRIMARY KEY (ID)";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			ps.setLong(1, 1000l);
			thrown.expect(SQLFeatureNotSupportedException.class);
			thrown.expectMessage("DDL statements may not be batched");
			ps.addBatch();
		}

		@Test
		public void testBatchedInsertStatementOnReadOnlyConnection() throws SQLException
		{
			thrown.expect(CloudSpannerSQLException.class);
			thrown.expectMessage(AbstractCloudSpannerStatement.NO_MUTATIONS_IN_READ_ONLY_MODE_EXCEPTION);
			String sql = "INSERT INTO FOO (ID, COL1, COL2) VALUES (?, ?, ?)";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			ps.addBatch();
			ps.getConnection().setReadOnly(true);
			ps.executeBatch();
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

		@Test
		public void testFormatDDLStatement() throws SQLException
		{
			try (CloudSpannerPreparedStatement ps = new CloudSpannerPreparedStatement("FOO", null, null))
			{
				Assert.assertEquals("CREATE TABLE TEST (ID INT64) PRIMARY KEY (ID)",
						ps.formatDDLStatement("CREATE TABLE TEST (ID INT64) PRIMARY KEY (ID)"));
				Assert.assertEquals("CREATE TABLE TEST (ID INT64) PRIMARY KEY (ID)",
						ps.formatDDLStatement("CREATE TABLE TEST (ID INT64, PRIMARY KEY (ID))"));
				Assert.assertEquals("CREATE TABLE TEST (ID INT64) PRIMARY KEY (ID)",
						ps.formatDDLStatement("CREATE TABLE TEST (ID INT64, PRIMARY KEY (ID))"));
				Assert.assertEquals("CREATE TABLE TEST (ID INT64) PRIMARY KEY (ID)",
						ps.formatDDLStatement("CREATE TABLE TEST\n\t(ID INT64,   PRIMARY  KEY  (ID) )"));
				Assert.assertEquals("CREATE TABLE TEST (Id INT64, Description String(100)) PRIMARY KEY (Id)", ps
						.formatDDLStatement("CREATE TABLE TEST (Id INT64, Description String(100)) PRIMARY KEY (Id)"));
				Assert.assertEquals("CREATE TABLE TEST (`Id` INT64, `Description` String(100)) PRIMARY KEY (`Id`)",
						ps.formatDDLStatement(
								"CREATE TABLE TEST (`Id` INT64, `Description` String(100)) PRIMARY KEY (`Id`)"));
				String sql = "CREATE TABLE TestTableViaDBeaver(\n" + "TestId INT64 NOT NULL,\n" + "Foo STRING(10)\n"
						+ ") PRIMARY KEY (TestId);";
				Assert.assertEquals(sql, ps.formatDDLStatement(sql));

				Assert.assertEquals("CREATE TABLE Account (id INT64 NOT NULL, name STRING(100)) primary key (id)",
						ps.formatDDLStatement(
								"CREATE TABLE Account (id INT64 NOT NULL, name STRING(100), primary key (id))"));
				Assert.assertEquals("CREATE TABLE `FOO` (`ID` INT64, `NAME` STRING(100)) PRIMARY KEY (ID)",
						ps.formatDDLStatement(
								"/* CREATE A TEST TABLE */\nCREATE TABLE `FOO` (`ID` INT64, `NAME` STRING(100)) PRIMARY KEY (ID)"));
				Assert.assertEquals("CREATE TABLE `FOO` (`ID` INT64, `NAME` STRING(100)) PRIMARY KEY (ID)",
						ps.formatDDLStatement(
								"-- CREATE A TEST TABLE \nCREATE TABLE `FOO` (`ID` INT64, `NAME` STRING(100)) PRIMARY KEY (ID)"));
			}
		}

		@Test
		public void testCommentedCreateTableStatement() throws SQLException
		{
			CloudSpannerPreparedStatementTest.testCreateTableStatement(
					"/* CREATE A TEST TABLE */\nCREATE TABLE `FOO` (`ID` INT64, `NAME` STRING(100)) PRIMARY KEY (ID)");
			CloudSpannerPreparedStatementTest.testCreateTableStatement(
					"-- CREATE A TEST TABLE\nCREATE TABLE `FOO` (`ID` INT64, `NAME` STRING(100)) PRIMARY KEY (ID)");
		}
	}

	public static class ParameterTests
	{
		@Test
		public void testParameters() throws SQLException, MalformedURLException
		{
			String sql = "INSERT INTO FOO (ID, COL1, COL2) VALUES (?, ?, ?)";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			ps.setArray(1, ps.getConnection().createArrayOf("INT64", new Long[] { 1L, 2L, 3L }));
			ps.setAsciiStream(2, new ByteArrayInputStream("TEST".getBytes()));
			ps.setAsciiStream(3, new ByteArrayInputStream("TEST".getBytes()), 4);
			ps.setAsciiStream(4, new ByteArrayInputStream("TEST".getBytes()), 4l);
			ps.setBigDecimal(5, BigDecimal.valueOf(1l));
			ps.setBinaryStream(6, new ByteArrayInputStream("TEST".getBytes()));
			ps.setBinaryStream(7, new ByteArrayInputStream("TEST".getBytes()), 4);
			ps.setBinaryStream(8, new ByteArrayInputStream("TEST".getBytes()), 4l);
			ps.setBlob(9, (Blob) null);
			ps.setBlob(10, new ByteArrayInputStream("TEST".getBytes()));
			ps.setBlob(11, new ByteArrayInputStream("TEST".getBytes()), 4l);
			ps.setBoolean(12, Boolean.TRUE);
			ps.setByte(13, (byte) 1);
			ps.setBytes(14, "TEST".getBytes());
			ps.setCharacterStream(15, new StringReader("TEST"));
			ps.setCharacterStream(16, new StringReader("TEST"), 4);
			ps.setCharacterStream(17, new StringReader("TEST"), 4l);
			ps.setClob(18, (Clob) null);
			ps.setClob(19, new StringReader("TEST"));
			ps.setClob(20, new StringReader("TEST"), 4l);
			ps.setDate(21, new Date(1000l));
			ps.setDate(22, new Date(1000l), Calendar.getInstance(TimeZone.getTimeZone("GMT")));
			ps.setDouble(23, 1d);
			ps.setFloat(24, 1f);
			ps.setInt(25, 1);
			ps.setLong(26, 1l);
			ps.setNCharacterStream(27, new StringReader("TEST"));
			ps.setNCharacterStream(28, new StringReader("TEST"), 4l);
			ps.setNClob(29, (NClob) null);
			ps.setNClob(30, new StringReader("TEST"));
			ps.setNClob(31, new StringReader("TEST"), 4l);
			ps.setNString(32, "TEST");
			ps.setNull(33, Types.BIGINT);
			ps.setNull(34, Types.BIGINT, "INT64");
			ps.setObject(35, "TEST");
			ps.setObject(36, "TEST", Types.NVARCHAR);
			ps.setObject(37, "TEST", Types.NVARCHAR, 20);
			ps.setRef(38, (Ref) null);
			ps.setRowId(39, (RowId) null);
			ps.setShort(40, (short) 1);
			ps.setSQLXML(41, (SQLXML) null);
			ps.setString(42, "TEST");
			ps.setTime(43, new Time(1000l));
			ps.setTime(44, new Time(1000l), Calendar.getInstance(TimeZone.getTimeZone("GMT")));
			ps.setTimestamp(45, new Timestamp(1000l));
			ps.setTimestamp(46, new Timestamp(1000l), Calendar.getInstance(TimeZone.getTimeZone("GMT")));
			ps.setUnicodeStream(47, new ByteArrayInputStream("TEST".getBytes()), 4);
			ps.setURL(48, new URL("http://www.googlecloudspanner.com"));

			CloudSpannerParameterMetaData pmd = ps.getParameterMetaData();
			Assert.assertEquals(48, pmd.getParameterCount());
			Assert.assertEquals(CloudSpannerArray.class.getName(), pmd.getParameterClassName(1));
			Assert.assertEquals(ByteArrayInputStream.class.getName(), pmd.getParameterClassName(2));
			Assert.assertEquals(ByteArrayInputStream.class.getName(), pmd.getParameterClassName(3));
			Assert.assertEquals(ByteArrayInputStream.class.getName(), pmd.getParameterClassName(4));
			Assert.assertEquals(BigDecimal.class.getName(), pmd.getParameterClassName(5));
			Assert.assertEquals(ByteArrayInputStream.class.getName(), pmd.getParameterClassName(6));
			Assert.assertEquals(ByteArrayInputStream.class.getName(), pmd.getParameterClassName(7));
			Assert.assertEquals(ByteArrayInputStream.class.getName(), pmd.getParameterClassName(8));
			Assert.assertNull(pmd.getParameterClassName(9));
			Assert.assertEquals(ByteArrayInputStream.class.getName(), pmd.getParameterClassName(10));
			Assert.assertEquals(ByteArrayInputStream.class.getName(), pmd.getParameterClassName(11));
			Assert.assertEquals(Boolean.class.getName(), pmd.getParameterClassName(12));
			Assert.assertEquals(Byte.class.getName(), pmd.getParameterClassName(13));
			Assert.assertEquals(byte[].class.getName(), pmd.getParameterClassName(14));
			Assert.assertEquals(StringReader.class.getName(), pmd.getParameterClassName(15));
			Assert.assertEquals(StringReader.class.getName(), pmd.getParameterClassName(16));
			Assert.assertEquals(StringReader.class.getName(), pmd.getParameterClassName(17));
			Assert.assertNull(pmd.getParameterClassName(18));
			Assert.assertEquals(StringReader.class.getName(), pmd.getParameterClassName(19));
			Assert.assertEquals(StringReader.class.getName(), pmd.getParameterClassName(20));
			Assert.assertEquals(Date.class.getName(), pmd.getParameterClassName(21));
			Assert.assertEquals(Date.class.getName(), pmd.getParameterClassName(22));
			Assert.assertEquals(Double.class.getName(), pmd.getParameterClassName(23));
			Assert.assertEquals(Float.class.getName(), pmd.getParameterClassName(24));
			Assert.assertEquals(Integer.class.getName(), pmd.getParameterClassName(25));
			Assert.assertEquals(Long.class.getName(), pmd.getParameterClassName(26));
			Assert.assertEquals(StringReader.class.getName(), pmd.getParameterClassName(27));
			Assert.assertEquals(StringReader.class.getName(), pmd.getParameterClassName(28));
			Assert.assertNull(pmd.getParameterClassName(29));
			Assert.assertEquals(StringReader.class.getName(), pmd.getParameterClassName(30));
			Assert.assertEquals(StringReader.class.getName(), pmd.getParameterClassName(31));
			Assert.assertEquals(String.class.getName(), pmd.getParameterClassName(32));
			Assert.assertEquals(Long.class.getName(), pmd.getParameterClassName(33));
			Assert.assertEquals(Long.class.getName(), pmd.getParameterClassName(34));
			Assert.assertEquals(String.class.getName(), pmd.getParameterClassName(35));
			Assert.assertEquals(String.class.getName(), pmd.getParameterClassName(36));
			Assert.assertEquals(String.class.getName(), pmd.getParameterClassName(37));
			Assert.assertNull(pmd.getParameterClassName(38));
			Assert.assertNull(pmd.getParameterClassName(39));
			Assert.assertEquals(Short.class.getName(), pmd.getParameterClassName(40));
			Assert.assertNull(pmd.getParameterClassName(41));
			Assert.assertEquals(String.class.getName(), pmd.getParameterClassName(42));
			Assert.assertEquals(Time.class.getName(), pmd.getParameterClassName(43));
			Assert.assertEquals(Time.class.getName(), pmd.getParameterClassName(44));
			Assert.assertEquals(Timestamp.class.getName(), pmd.getParameterClassName(45));
			Assert.assertEquals(Timestamp.class.getName(), pmd.getParameterClassName(46));
			Assert.assertEquals(ByteArrayInputStream.class.getName(), pmd.getParameterClassName(47));
			Assert.assertEquals(URL.class.getName(), pmd.getParameterClassName(48));

			ps.clearParameters();
			pmd = ps.getParameterMetaData();
			// 3 because the statement has 3 parameters defined in the query
			Assert.assertEquals(3, pmd.getParameterCount());
		}
	}

	public static class SelectStatementTests
	{
		@Rule
		public ExpectedException thrown = ExpectedException.none();

		@Test
		public void testSelect1() throws SQLException
		{
			String sql = "SELECT 1";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			try (ResultSet rs = ps.executeQuery())
			{
			}
		}

		@Test
		public void testRemoveSingleLineComment() throws SQLException
		{
			String sql = "-- test adding not null column\nCREATE TABLE TEST (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			String sqlWithoutComments = ps.removeComments(sql);
			Assert.assertEquals("CREATE TABLE TEST (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)",
					sqlWithoutComments);
		}

		@Test
		public void testRemoveMultiLineComment() throws SQLException
		{
			String sql = "/* test adding not null column */\nCREATE TABLE TEST (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			String sqlWithoutComments = ps.removeComments(sql);
			Assert.assertEquals("CREATE TABLE TEST (ID INT64 NOT NULL, NAME STRING(100)) PRIMARY KEY (ID)",
					sqlWithoutComments);
		}

		@Test
		public void testExecuteQuerySQL() throws SQLException, MalformedURLException
		{
			thrown.expect(SQLException.class);
			thrown.expectMessage("The executeQuery(String sql)-method may not be called on a PreparedStatement");
			String sql = "SELECT * FROM FOO";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			try (ResultSet rs = ps.executeQuery(sql))
			{
			}
		}

		@Test
		public void testInvalidSQL() throws SQLException, MalformedURLException
		{
			thrown.expect(SQLException.class);
			thrown.expectMessage(CloudSpannerPreparedStatement.PARSE_ERROR);
			String sql = "SELECT * FOO";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			try (ResultSet rs = ps.executeQuery())
			{
			}
		}

		@Test
		public void testExecuteQueryWithDML() throws SQLException, MalformedURLException
		{
			thrown.expect(SQLException.class);
			thrown.expectMessage("SQL statement not suitable for executeQuery. Expected SELECT-statement.");
			String sql = "DELETE FROM FOO";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			try (ResultSet rs = ps.executeQuery())
			{
			}
		}

		@Test
		public void testSimpleSelect() throws SQLException, MalformedURLException
		{
			String sql = "SELECT * FROM FOO";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			Assert.assertFalse(ps.isForceSingleUseReadContext());
			try (ResultSet rs = ps.executeQuery())
			{
			}
		}

		@Test
		public void testSelectWithParameters() throws SQLException, MalformedURLException
		{
			String sql = "SELECT * FROM FOO WHERE ID=?";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			ps.setLong(1, 1000L);
			try (ResultSet rs = ps.executeQuery())
			{
			}
		}

		@Test
		public void testSelectWithNullValue() throws SQLException, MalformedURLException
		{
			String sql = "SELECT * FROM FOO WHERE COL1 IS NULL";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			try (ResultSet rs = ps.executeQuery())
			{
			}
		}

		@Test
		public void testSelectWithSubSelect() throws SQLException, MalformedURLException
		{
			String sql = "SELECT * FROM FOO WHERE ID IN (SELECT COL1 FROM BAR WHERE COL2=?)";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			ps.setLong(1, 1000L);
			try (ResultSet rs = ps.executeQuery())
			{
			}
		}

		@Test
		public void testSelectWithForceIndex() throws SQLException, MalformedURLException
		{
			String sql = "SELECT * FROM FOO@{FORCE_INDEX=TEST_INDEX} WHERE ID=?";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			ps.setLong(1, 1000L);
			try (ResultSet rs = ps.executeQuery())
			{
			}
		}

		@Test
		public void testSelectWithLimitAndOffset() throws SQLException, MalformedURLException
		{
			String sql = "SELECT * FROM FOO LIMIT ? OFFSET ?";
			CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
			ps.setLong(1, 1000l);
			ps.setLong(2, 10000l);
			try (ResultSet rs = ps.executeQuery())
			{
			}

			com.google.cloud.spanner.Statement.Builder res = null;
			try
			{
				Statement statement = CCJSqlParserUtil.parse(ps.sanitizeSQL(sql));
				Method createSelectBuilder = CloudSpannerPreparedStatement.class
						.getDeclaredMethod("createSelectBuilder", Statement.class, String.class);
				createSelectBuilder.setAccessible(true);
				res = (com.google.cloud.spanner.Statement.Builder) createSelectBuilder.invoke(ps, statement, sql);
			}
			catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
					| JSQLParserException e)
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
			// Check the resulting builder.
			com.google.cloud.spanner.Statement googleStatement = res.build();
			String googleSql = googleStatement.getSql();
			long param1 = googleStatement.getParameters().get("p1").getInt64();
			long param2 = googleStatement.getParameters().get("p2").getInt64();
			Assert.assertEquals("SELECT * FROM FOO LIMIT @p1 OFFSET @p2", googleSql);
			Assert.assertEquals(1000l, param1);
			Assert.assertEquals(10000l, param2);
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
			throw new CloudSpannerSQLException("Could not parse SQL statement", Code.INVALID_ARGUMENT, e);
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
		CloudSpannerPreparedStatement ps = new CloudSpannerPreparedStatement(sql, mock(CloudSpannerConnection.class),
				mock(DatabaseClient.class));
		try
		{
			Method isDDLStatement = CloudSpannerPreparedStatement.class.getDeclaredMethod("isDDLStatement");
			isDDLStatement.setAccessible(true);
			res = (boolean) isDDLStatement.invoke(ps);
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
		CloudSpannerPreparedStatement ps = CloudSpannerTestObjects.createPreparedStatement(sql);
		try
		{
			Method createMutations = ps.getClass().getDeclaredMethod("createMutations");
			createMutations.setAccessible(true);
			mutations = (Mutations) createMutations.invoke(ps);
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
