package nl.topicus.jdbc.statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.internal.stubbing.answers.Returns;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.cloud.spanner.Type;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDatabaseMetaData;
import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;
import nl.topicus.jdbc.resultset.CloudSpannerResultSet;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerParameterMetaDataTest
{
	private static final String TYPE_NAME_OTHER = "Other";

	private CloudSpannerPreparedStatement createSelectStatement() throws SQLException
	{
		String sql = "SELECT COL1, COL2, COL3 FROM FOO WHERE COL1<? AND COL4=?";
		return createStatement(sql);
	}

	private CloudSpannerPreparedStatement createInsertStatement() throws SQLException
	{
		return createStatement("INSERT INTO FOO (COL1, COL2, COL3) VALUES (?, ?, ?)");
	}

	private static class ColumnsNextAnswer implements Answer<Boolean>
	{
		int nextCalled = 0;

		@Override
		public Boolean answer(InvocationOnMock invocation) throws Throwable
		{
			nextCalled++;
			if (nextCalled > 3)
				return false;
			return true;
		}
	}

	private CloudSpannerPreparedStatement createStatement(String sql) throws SQLException
	{
		CloudSpannerConnection connection = mock(CloudSpannerConnection.class);
		CloudSpannerDatabaseMetaData metadata = mock(CloudSpannerDatabaseMetaData.class);
		CloudSpannerResultSet columns = mock(CloudSpannerResultSet.class);
		TableKeyMetaData table = mock(TableKeyMetaData.class);
		when(table.getKeyColumns()).thenReturn(Arrays.asList("COL1"));
		when(connection.getTable("FOO")).thenReturn(table);
		when(connection.getMetaData()).thenAnswer(new Returns(metadata));
		when(metadata.getColumns(null, null, "FOO", null)).thenReturn(columns);
		final ColumnsNextAnswer next = new ColumnsNextAnswer();
		when(columns.next()).thenAnswer(next);

		when(columns.getString("COLUMN_NAME")).thenAnswer(new Answer<String>()
		{

			@Override
			public String answer(InvocationOnMock invocation) throws Throwable
			{
				return Arrays.asList("COL1", "COL2", "COL3").get(next.nextCalled - 1);
			}
		});
		when(columns.getInt("COLUMN_SIZE")).thenAnswer(new Answer<Integer>()
		{

			@Override
			public Integer answer(InvocationOnMock invocation) throws Throwable
			{
				return Arrays.asList(8, 50, 100).get(next.nextCalled - 1);
			}
		});
		when(columns.getInt("DATA_TYPE")).thenAnswer(new Answer<Integer>()
		{

			@Override
			public Integer answer(InvocationOnMock invocation) throws Throwable
			{
				return Arrays.asList(Types.BIGINT, Types.NVARCHAR, Types.NVARCHAR).get(next.nextCalled - 1);
			}
		});
		when(columns.getInt("NULLABLE")).thenAnswer(new Answer<Integer>()
		{

			@Override
			public Integer answer(InvocationOnMock invocation) throws Throwable
			{
				return Arrays.asList(ParameterMetaData.parameterNoNulls, ParameterMetaData.parameterNoNulls,
						ParameterMetaData.parameterNullable).get(next.nextCalled - 1);
			}
		});

		return new CloudSpannerPreparedStatement(sql, connection, null);
	}

	@Test
	public void testGetParameterCount() throws SQLException
	{
		try (CloudSpannerPreparedStatement ps = createSelectStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(2, metadata.getParameterCount());
		}
		try (CloudSpannerPreparedStatement ps = createInsertStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(3, metadata.getParameterCount());
		}
	}

	@Test
	public void testIsNullable() throws SQLException
	{
		try (CloudSpannerPreparedStatement ps = createSelectStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(ParameterMetaData.parameterNoNulls, metadata.isNullable(1));
			assertEquals(ParameterMetaData.parameterNullableUnknown, metadata.isNullable(2));
		}
		try (CloudSpannerPreparedStatement ps = createInsertStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(ParameterMetaData.parameterNoNulls, metadata.isNullable(1));
			assertEquals(ParameterMetaData.parameterNoNulls, metadata.isNullable(2));
			assertEquals(ParameterMetaData.parameterNullable, metadata.isNullable(3));
		}
	}

	@Test
	public void testIsSigned() throws SQLException
	{
		try (CloudSpannerPreparedStatement ps = createSelectStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(true, metadata.isSigned(1));
			assertEquals(false, metadata.isSigned(2));
			ps.setLong(2, 1000l);
			assertEquals(true, metadata.isSigned(2));
		}
		try (CloudSpannerPreparedStatement ps = createInsertStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(true, metadata.isSigned(1));
			assertEquals(false, metadata.isSigned(2));
			assertEquals(false, metadata.isSigned(3));
			ps.setLong(1, 1000l);
			assertEquals(true, metadata.isSigned(1));
		}
	}

	@Test
	public void testGetPrecision() throws SQLException
	{
		try (CloudSpannerPreparedStatement ps = createSelectStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(8, metadata.getPrecision(1));
			assertEquals(0, metadata.getPrecision(2));
		}
		try (CloudSpannerPreparedStatement ps = createInsertStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(8, metadata.getPrecision(1));
			assertEquals(50, metadata.getPrecision(2));
			assertEquals(100, metadata.getPrecision(3));
		}
	}

	@Test
	public void testGetScale() throws SQLException
	{
		try (CloudSpannerPreparedStatement ps = createSelectStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(0, metadata.getScale(1));
			assertEquals(0, metadata.getScale(2));
		}
		try (CloudSpannerPreparedStatement ps = createInsertStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(0, metadata.getScale(1));
			assertEquals(0, metadata.getScale(2));
			assertEquals(0, metadata.getScale(3));
		}
	}

	@Test
	public void testGetParameterType() throws SQLException
	{
		try (CloudSpannerPreparedStatement ps = createSelectStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(Types.BIGINT, metadata.getParameterType(1));
			assertEquals(Types.OTHER, metadata.getParameterType(2));
			ps.setBoolean(1, Boolean.TRUE);
			assertEquals(Types.BOOLEAN, metadata.getParameterType(1));
			ps.setByte(1, (byte) 1);
			assertEquals(Types.TINYINT, metadata.getParameterType(1));
			ps.setShort(1, (short) 1);
			assertEquals(Types.SMALLINT, metadata.getParameterType(1));
			ps.setInt(1, 1);
			assertEquals(Types.INTEGER, metadata.getParameterType(1));
			ps.setLong(1, 1l);
			assertEquals(Types.BIGINT, metadata.getParameterType(1));
			ps.setBigDecimal(1, BigDecimal.ONE);
			assertEquals(Types.DECIMAL, metadata.getParameterType(1));
			ps.setFloat(1, 1f);
			assertEquals(Types.FLOAT, metadata.getParameterType(1));
			ps.setDouble(1, 1d);
			assertEquals(Types.DOUBLE, metadata.getParameterType(1));
			ps.setDate(1, new Date(1000l));
			assertEquals(Types.DATE, metadata.getParameterType(1));
			ps.setTimestamp(1, new Timestamp(1000l));
			assertEquals(Types.TIMESTAMP, metadata.getParameterType(1));
			ps.setTime(1, new Time(1000l));
			assertEquals(Types.TIME, metadata.getParameterType(1));
			ps.setString(1, "FOO");
			assertEquals(Types.NVARCHAR, metadata.getParameterType(1));
			ps.setBytes(1, "FOO".getBytes());
			assertEquals(Types.BINARY, metadata.getParameterType(1));
		}
		try (CloudSpannerPreparedStatement ps = createInsertStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(Types.BIGINT, metadata.getParameterType(1));
			assertEquals(Types.NVARCHAR, metadata.getParameterType(2));
			assertEquals(Types.NVARCHAR, metadata.getParameterType(3));
		}
	}

	@Test
	public void testGetParameterTypeName() throws SQLException
	{
		try (CloudSpannerPreparedStatement ps = createSelectStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals("INT64", metadata.getParameterTypeName(1));
			assertEquals(TYPE_NAME_OTHER, metadata.getParameterTypeName(2));
		}
		try (CloudSpannerPreparedStatement ps = createInsertStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(Type.int64().getCode().name(), metadata.getParameterTypeName(1));
			assertEquals(Type.string().getCode().name(), metadata.getParameterTypeName(2));
			assertEquals(Type.string().getCode().name(), metadata.getParameterTypeName(3));
		}
	}

	@Test
	public void testGetParameterClassName() throws SQLException
	{
		try (CloudSpannerPreparedStatement ps = createSelectStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(Long.class.getName(), metadata.getParameterClassName(1));
			assertNull(metadata.getParameterClassName(2));
		}
		try (CloudSpannerPreparedStatement ps = createInsertStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(Long.class.getName(), metadata.getParameterClassName(1));
			assertEquals(String.class.getName(), metadata.getParameterClassName(2));
			assertEquals(String.class.getName(), metadata.getParameterClassName(3));
		}
	}

	@Test
	public void testGetParameterMode() throws SQLException
	{
		try (CloudSpannerPreparedStatement ps = createSelectStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(ParameterMetaData.parameterModeIn, metadata.getParameterMode(1));
			assertEquals(ParameterMetaData.parameterModeIn, metadata.getParameterMode(2));
		}
		try (CloudSpannerPreparedStatement ps = createInsertStatement())
		{
			ParameterMetaData metadata = ps.getParameterMetaData();
			assertEquals(ParameterMetaData.parameterModeIn, metadata.getParameterMode(1));
			assertEquals(ParameterMetaData.parameterModeIn, metadata.getParameterMode(2));
			assertEquals(ParameterMetaData.parameterModeIn, metadata.getParameterMode(3));
		}
	}

	@Test
	public void testToString() throws SQLException
	{
		try (CloudSpannerPreparedStatement ps = createSelectStatement())
		{
			String str = ps.getParameterMetaData().toString();
			assertNotNull(str);
			assertNotEquals("", str);
			assertEquals(-1, str.indexOf("Error while fetching parameter metadata"));
		}
		try (CloudSpannerPreparedStatement ps = createInsertStatement())
		{
			String str = ps.getParameterMetaData().toString();
			assertNotNull(str);
			assertNotEquals("", str);
			assertEquals(-1, str.indexOf("Error while fetching parameter metadata"));
		}
	}

	@Test
	public void testGetParameterMetaDataUpdateStatement() throws SQLException
	{
		String sqlStatement = "UPDATE `FOO` SET `COL1` = ?, `COL2` = ? WHERE `COL3` = ?";
		CloudSpannerPreparedStatement preparedStatement = createStatement(sqlStatement);
		CloudSpannerParameterMetaData metadata = preparedStatement.getParameterMetaData();
		assertNotNull(metadata);
		assertEquals(Types.BIGINT, metadata.getParameterType(1));
		assertEquals(Types.NVARCHAR, metadata.getParameterType(2));
		assertEquals(Types.NVARCHAR, metadata.getParameterType(3));
	}

	@Test
	public void testGetParameterMetaDataDeleteStatement() throws SQLException
	{
		String sqlStatement = "DELETE FROM `FOO` WHERE `COL1` = ?";
		CloudSpannerPreparedStatement preparedStatement = createStatement(sqlStatement);
		CloudSpannerParameterMetaData metadata = preparedStatement.getParameterMetaData();
		assertNotNull(metadata);
		assertEquals(Types.BIGINT, metadata.getParameterType(1));
	}

	@Test
	public void testGetParameterMetaDataBulkDeleteStatement() throws SQLException
	{
		String sqlStatement = "DELETE FROM `FOO` WHERE `COL3` > ?";
		CloudSpannerPreparedStatement preparedStatement = createStatement(sqlStatement);
		CloudSpannerParameterMetaData metadata = preparedStatement.getParameterMetaData();
		assertNotNull(metadata);
		assertEquals(Types.NVARCHAR, metadata.getParameterType(1));
	}

	@Test
	public void testGetParameterMetaDataInsertStatement() throws SQLException
	{
		CloudSpannerPreparedStatement preparedStatement = createInsertStatement();
		CloudSpannerParameterMetaData metadata = preparedStatement.getParameterMetaData();
		assertNotNull(metadata);
		assertEquals(3, metadata.getParameterCount());
		assertEquals(Types.BIGINT, metadata.getParameterType(1));
		assertEquals(Types.NVARCHAR, metadata.getParameterType(2));
		assertEquals(Types.NVARCHAR, metadata.getParameterType(3));
	}

	@Test
	public void testGetParameterMetaDataBulkInsertStatement() throws SQLException
	{
		CloudSpannerPreparedStatement preparedStatement = createStatement(
				"INSERT INTO FOO (COL1, COL2, COL3) SELECT 1, 'Two', 'Three' FROM FOO WHERE COL1=? AND COL2=? AND COL3 LIKE ?");
		CloudSpannerParameterMetaData metadata = preparedStatement.getParameterMetaData();
		assertNotNull(metadata);
		assertEquals(3, metadata.getParameterCount());
		assertEquals(Types.BIGINT, metadata.getParameterType(1));
		assertEquals(Types.NVARCHAR, metadata.getParameterType(2));
		assertEquals(Types.NVARCHAR, metadata.getParameterType(3));
	}

	@Test
	public void testGetParameterMetaDataSelectStatement() throws SQLException
	{
		CloudSpannerPreparedStatement preparedStatement = createStatement(
				"SELECT * FROM FOO WHERE COL1=? AND COL2=? AND COL3 LIKE ?");
		CloudSpannerParameterMetaData metadata = preparedStatement.getParameterMetaData();
		assertNotNull(metadata);
		assertEquals(3, metadata.getParameterCount());
		assertEquals(Types.BIGINT, metadata.getParameterType(1));
		assertEquals(Types.NVARCHAR, metadata.getParameterType(2));
		assertEquals(Types.NVARCHAR, metadata.getParameterType(3));
	}

}
