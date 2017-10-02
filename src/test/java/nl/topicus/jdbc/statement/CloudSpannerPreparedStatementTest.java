package nl.topicus.jdbc.statement;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
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
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
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
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.cloud.spanner.Value;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import nl.topicus.jdbc.CloudSpannerArray;
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
			Mutation updateMutation = getMutation("UPDATE BAR SET COL1=1, COL2=2 WHERE ID1=1 AND ID2=1");
			Assert.assertNotNull(updateMutation);
			Assert.assertEquals(Op.UPDATE, updateMutation.getOperation());
			Assert.assertEquals("BAR", updateMutation.getTable());
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

	public static class ParameterTests
	{
		@Test
		public void testParameters() throws SQLException, MalformedURLException
		{
			String sql = "INSERT INTO FOO (ID, COL1, COL2) VALUES (?, ?, ?)";
			CloudSpannerPreparedStatement ps = createPreparedStatement(sql);
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
		CloudSpannerPreparedStatement ps = createPreparedStatement(sql);
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

	private static CloudSpannerPreparedStatement createPreparedStatement(String sql) throws SQLException
	{
		CloudSpannerConnection connection = Mockito.mock(CloudSpannerConnection.class);
		Mockito.when(connection.createArrayOf(Mockito.anyString(), Mockito.any())).thenCallRealMethod();
		CloudSpannerDatabaseMetaData metadata = Mockito.mock(CloudSpannerDatabaseMetaData.class);
		CloudSpannerResultSet resultSetFoo = Mockito.mock(CloudSpannerResultSet.class);
		TableKeyMetaData tableFoo = Mockito.mock(TableKeyMetaData.class);
		Mockito.when(connection.getMetaData()).thenReturn(metadata);
		Mockito.when(metadata.getPrimaryKeys(null, null, "FOO")).thenReturn(resultSetFoo);
		Mockito.when(resultSetFoo.next()).thenReturn(true, false);
		Mockito.when(resultSetFoo.getString("COLUMN_NAME")).thenReturn("ID");
		Mockito.when(connection.getTable("FOO")).thenReturn(tableFoo);
		Mockito.when(tableFoo.getKeyColumns()).thenReturn(Arrays.asList("ID"));

		CloudSpannerResultSet resultSetBar = Mockito.mock(CloudSpannerResultSet.class);
		TableKeyMetaData tableBar = Mockito.mock(TableKeyMetaData.class);
		Mockito.when(metadata.getPrimaryKeys(null, null, "BAR")).thenReturn(resultSetBar);
		Mockito.when(resultSetBar.next()).thenReturn(true, true, false);
		Mockito.when(resultSetBar.getString("COLUMN_NAME")).thenReturn("ID1", "ID2");
		Mockito.when(connection.getTable("BAR")).thenReturn(tableBar);
		Mockito.when(tableBar.getKeyColumns()).thenReturn(Arrays.asList("ID1", "ID2"));

		CloudSpannerResultSet fooColumns = Mockito.mock(CloudSpannerResultSet.class);
		Mockito.when(fooColumns.next()).thenReturn(true, true, true, false);
		Mockito.when(fooColumns.getString("COLUMN_NAME")).thenReturn("ID", "COL1", "COL2");
		Mockito.when(fooColumns.getInt("COLUMN_SIZE")).thenReturn(8, 50, 100);
		Mockito.when(fooColumns.getInt("DATA_TYPE")).thenReturn(Types.BIGINT, Types.NVARCHAR, Types.NVARCHAR);
		Mockito.when(fooColumns.getInt("NULLABLE")).thenReturn(ResultSetMetaData.columnNoNulls,
				ResultSetMetaData.columnNoNulls, ResultSetMetaData.columnNullable);
		Mockito.when(metadata.getColumns(null, null, "FOO", null)).thenReturn(fooColumns);

		return new CloudSpannerPreparedStatement(sql, connection, null);
	}

}
