package nl.topicus.jdbc.resultset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.Returns;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import com.google.common.base.Preconditions;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.CloudSpannerDatabaseMetaData;
import nl.topicus.jdbc.statement.CloudSpannerStatement;

public class CloudSpannerResultSetMetaDataTest
{
	private static final String SELECT_ALL_FROM_FOO = "SELECT *, (2+2) AS CALCULATED FROM FOO";

	private static class TestColumn
	{
		private final Type type;
		private final String name;
		private final int nullable;
		private final int size;
		private final boolean calculated;

		private TestColumn(Type type, String name, Integer nulls, int size, boolean calculated)
		{
			Preconditions.checkNotNull(type);
			Preconditions.checkNotNull(name);
			Preconditions.checkNotNull(nulls);
			this.type = type;
			this.name = name;
			this.nullable = nulls;
			this.size = size;
			this.calculated = calculated;
		}

		private boolean isTableColumn()
		{
			return !calculated;
		}

		private static class Builder
		{
			private Type type;
			private String name;
			private Integer nulls;
			private int size = 0;
			private boolean calculated = false;

			public static Builder getBuilder()
			{
				return new Builder();
			}

			private TestColumn build()
			{
				return new TestColumn(type, name, nulls, size, calculated);
			}

			private Builder withType(Type type)
			{
				this.type = type;
				return this;
			}

			private Builder withName(String name)
			{
				this.name = name;
				return this;
			}

			private Builder withNotNull()
			{
				this.nulls = ResultSetMetaData.columnNoNulls;
				return this;
			}

			private Builder withNullable()
			{
				this.nulls = ResultSetMetaData.columnNullable;
				return this;
			}

			private Builder withNullableUnknown()
			{
				this.nulls = ResultSetMetaData.columnNullableUnknown;
				return this;
			}

			private Builder withSize(int size)
			{
				this.size = size;
				return this;
			}

			private Builder withCalculated(boolean calculated)
			{
				this.calculated = calculated;
				return this;
			}
		}
	}

	private static final List<TestColumn> TEST_COLUMNS = createTestColumns();
	private CloudSpannerResultSetMetaData subject;
	private CloudSpannerConnection connection;

	@Before
	public void setup() throws SQLException
	{
		connection = mock(CloudSpannerConnection.class);
		CloudSpannerStatement statement = mock(CloudSpannerStatement.class);
		CloudSpannerResultSet resultSet = getFooTestResultSet(statement);
		CloudSpannerDatabaseMetaData metadata = mock(CloudSpannerDatabaseMetaData.class);
		when(metadata.getColumns(eq(""), eq(""), eq("FOO"), any())).then(new Answer<CloudSpannerResultSet>()
		{
			@Override
			public CloudSpannerResultSet answer(InvocationOnMock invocation) throws Throwable
			{
				return createFooColumnsResultSet(statement, invocation.getArgument(3));
			}
		});
		when(metadata.getPrimaryKeys("", "", "FOO")).then(new Answer<CloudSpannerResultSet>()
		{
			@Override
			public CloudSpannerResultSet answer(InvocationOnMock invocation) throws Throwable
			{
				return createFooPrimaryKeysResultSet(statement);
			}
		});
		when(connection.getMetaData()).then(new Returns(metadata));
		when(connection.isReportDefaultSchemaAsNull()).thenCallRealMethod();
		when(connection.setReportDefaultSchemaAsNull(anyBoolean())).thenCallRealMethod();
		when(connection.getSchema()).thenCallRealMethod();
		when(connection.getCatalog()).thenCallRealMethod();
		when(statement.getConnection()).then(new Returns(connection));

		subject = resultSet.getMetaData();
	}

	private static List<TestColumn> createTestColumns()
	{
		List<TestColumn> res = new ArrayList<>();
		int index = 1;
		for (Type type : getAllTypes())
		{
			TestColumn.Builder builder = TestColumn.Builder.getBuilder();
			builder.withName("COL" + index).withType(type).withSize(getDefaultSize(type));
			if (index % 2 == 1)
				builder.withNotNull();
			else
				builder.withNullable();
			res.add(builder.build());
			index++;
		}
		TestColumn.Builder builder = TestColumn.Builder.getBuilder();
		builder.withName("CALCULATED").withType(Type.int64()).withNullableUnknown().withCalculated(true);
		res.add(builder.build());
		return res;
	}

	private static int getDefaultSize(Type type)
	{
		if (type == Type.string())
			return 100;
		return 0;
	}

	private static List<Type> getAllTypes()
	{
		List<Type> types = new ArrayList<>();
		types.add(Type.bool());
		types.add(Type.bytes());
		types.add(Type.date());
		types.add(Type.float64());
		types.add(Type.int64());
		types.add(Type.string());
		types.add(Type.timestamp());
		types.addAll(types.stream().map(Type::array).collect(Collectors.toList()));

		return types;
	}

	private CloudSpannerResultSet getFooTestResultSet(CloudSpannerStatement statement)
	{
		List<Struct> rows = new ArrayList<>(4);
		for (int row = 1; row <= 4; row++)
		{
			Struct.Builder builder = Struct.newBuilder();
			for (TestColumn col : TEST_COLUMNS)
			{
				builder.add(col.name, getDefaultValue(col.type, row));
			}
			rows.add(builder.build());
		}
		StructField[] fields = new StructField[TEST_COLUMNS.size()];
		int index = 0;
		for (TestColumn col : TEST_COLUMNS)
		{
			fields[index] = StructField.of(col.name, col.type);
			index++;
		}

		ResultSet rs = ResultSets.forRows(Type.struct(fields), rows);
		return new CloudSpannerResultSet(statement, rs, SELECT_ALL_FROM_FOO);
	}

	private Value getDefaultValue(Type type, int row)
	{
		if (type == Type.bool())
			return Value.bool(Boolean.TRUE);
		if (type == Type.bytes())
			return Value.bytes(ByteArray.copyFrom("test byte array " + row));
		if (type == Type.date())
			return Value.date(com.google.cloud.Date.fromYearMonthDay(2018, 4, 1));
		if (type == Type.float64())
			return Value.float64(123.45D);
		if (type == Type.int64())
			return Value.int64(12345L);
		if (type == Type.string())
			return Value.string("test value " + row);
		if (type == Type.timestamp())
			return Value.timestamp(com.google.cloud.Timestamp.now());

		if (type.getCode() == Code.ARRAY && type.getArrayElementType() == Type.bool())
			return Value.boolArray(Arrays.asList(Boolean.TRUE, Boolean.FALSE));
		if (type.getCode() == Code.ARRAY && type.getArrayElementType() == Type.bytes())
			return Value.bytesArray(Arrays.asList(ByteArray.copyFrom("test byte array " + row),
					ByteArray.copyFrom("test byte array " + row)));
		if (type.getCode() == Code.ARRAY && type.getArrayElementType() == Type.date())
			return Value.dateArray(Arrays.asList(com.google.cloud.Date.fromYearMonthDay(2018, 4, 1),
					com.google.cloud.Date.fromYearMonthDay(2018, 4, 2)));
		if (type.getCode() == Code.ARRAY && type.getArrayElementType() == Type.float64())
			return Value.float64Array(Arrays.asList(123.45D, 543.21D));
		if (type.getCode() == Code.ARRAY && type.getArrayElementType() == Type.int64())
			return Value.int64Array(Arrays.asList(12345L, 54321L));
		if (type.getCode() == Code.ARRAY && type.getArrayElementType() == Type.string())
			return Value.stringArray(Arrays.asList("test value " + row, "test value " + row));
		if (type.getCode() == Code.ARRAY && type.getArrayElementType() == Type.timestamp())
			return Value
					.timestampArray(Arrays.asList(com.google.cloud.Timestamp.now(), com.google.cloud.Timestamp.now()));
		return null;
	}

	private List<TestColumn> tableColumns()
	{
		return TEST_COLUMNS.stream().filter(TestColumn::isTableColumn).collect(Collectors.toList());
	}

	private TestColumn getTestColumn(String colName)
	{
		return tableColumns().stream().filter(col -> col.name.equalsIgnoreCase(colName)).findFirst().orElse(null);
	}

	private CloudSpannerResultSet createFooColumnsResultSet(CloudSpannerStatement statement, String colName)
	{
		List<TestColumn> columns = colName == null ? tableColumns() : Arrays.asList(getTestColumn(colName));
		List<Struct> rows = new ArrayList<>(columns.size());
		for (TestColumn col : columns)
		{
			rows.add(Struct.newBuilder().add("COLUMN_NAME", Value.string(col.name))
					.add("NULLABLE", Value.int64(col.nullable)).add("COLUMN_SIZE", Value.int64(col.size)).build());
		}
		ResultSet rs = ResultSets.forRows(Type.struct(StructField.of("COLUMN_NAME", Type.string()),
				StructField.of("NULLABLE", Type.int64()), StructField.of("COLUMN_SIZE", Type.int64())), rows);
		return new CloudSpannerResultSet(statement, rs, null);
	}

	private CloudSpannerResultSet createFooPrimaryKeysResultSet(CloudSpannerStatement statement)
	{
		List<Struct> rows = new ArrayList<>(1);
		rows.add(Struct.newBuilder().add("COLUMN_NAME", Value.string(tableColumns().get(0).name)).build());
		ResultSet rs = ResultSets.forRows(Type.struct(StructField.of("COLUMN_NAME", Type.string())), rows);
		return new CloudSpannerResultSet(statement, rs, null);
	}

	@Test
	public void testGetColumnCount() throws SQLException
	{
		assertEquals(TEST_COLUMNS.size(), subject.getColumnCount());
	}

	@Test
	public void testIsAutoIncrement() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			assertEquals(false, subject.isAutoIncrement(i));
		}
	}

	@Test
	public void testIsCaseSensitive() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			Type type = TEST_COLUMNS.get(i - 1).type;
			assertEquals(type == Type.string() || type == Type.bytes(), subject.isCaseSensitive(i));
		}
	}

	@Test
	public void testIsSearchable() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			// The last column is calculated
			assertEquals(i != TEST_COLUMNS.size(), subject.isSearchable(i));
		}
	}

	@Test
	public void testIsCurrency() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			assertEquals(false, subject.isCurrency(i));
		}
	}

	@Test
	public void testIsNullable() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			assertEquals(TEST_COLUMNS.get(i - 1).nullable, subject.isNullable(i));
		}
	}

	@Test
	public void testIsSigned() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			Type type = TEST_COLUMNS.get(i - 1).type;
			if (type == Type.int64() || type == Type.float64())
			{
				assertTrue(subject.isSigned(i));
			}
			else
			{
				assertFalse(subject.isSigned(i));
			}
		}
	}

	@Test
	public void testGetColumnDisplaySize() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			assertEquals(getDefaultDisplaySize(TEST_COLUMNS.get(i - 1).type, i), subject.getColumnDisplaySize(i));
		}
	}

	private int getDefaultDisplaySize(Type type, int column) throws SQLException
	{
		if (type.getCode() == Code.ARRAY)
			return 50;
		if (type == Type.bool())
			return 5;
		if (type == Type.bytes())
			return 50;
		if (type == Type.date())
			return 10;
		if (type == Type.float64())
			return 14;
		if (type == Type.int64())
			return 10;
		if (type == Type.string())
		{
			int length = subject.getPrecision(column);
			return length == 0 ? 50 : length;
		}
		if (type == Type.timestamp())
			return 16;
		return 10;
	}

	@Test
	public void testGetColumnLabel() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			assertEquals(TEST_COLUMNS.get(i - 1).name, subject.getColumnLabel(i));
		}
	}

	@Test
	public void testGetColumnName() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			assertEquals(TEST_COLUMNS.get(i - 1).name, subject.getColumnName(i));
		}
	}

	@Test
	public void testGetSchemaName() throws SQLException
	{
		boolean original = connection.isReportDefaultSchemaAsNull();
		connection.setReportDefaultSchemaAsNull(false);
		assertEquals("", subject.getSchemaName(1));
		connection.setReportDefaultSchemaAsNull(true);
		assertNull(subject.getSchemaName(1));
		connection.setReportDefaultSchemaAsNull(original);
	}

	@Test
	public void testGetPrecision() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			assertEquals(getPrecision(TEST_COLUMNS.get(i - 1)), subject.getPrecision(i));
		}
	}

	private int getPrecision(TestColumn col)
	{
		if (col.type == Type.bool())
			return 1;
		if (col.type == Type.date())
			return 10;
		if (col.type == Type.float64())
			return 14;
		if (col.type == Type.int64())
			return 10;
		if (col.type == Type.timestamp())
			return 24;
		if (col.isTableColumn())
			return col.size;
		return 0;
	}

	@Test
	public void testGetScale() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			assertEquals(getScale(TEST_COLUMNS.get(i - 1)), subject.getScale(i));
		}
	}

	private int getScale(TestColumn col)
	{
		if (col.type == Type.float64())
			return 15;
		return 0;
	}

	@Test
	public void testGetTableName() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			if (TEST_COLUMNS.get(i - 1).isTableColumn())
				assertEquals("FOO", subject.getTableName(i));
			else
				assertEquals("", subject.getTableName(i));
		}
	}

	@Test
	public void testGetCatalogName() throws SQLException
	{
		boolean original = connection.isReportDefaultSchemaAsNull();
		connection.setReportDefaultSchemaAsNull(false);
		assertEquals("", subject.getCatalogName(1));
		connection.setReportDefaultSchemaAsNull(true);
		assertNull(subject.getCatalogName(1));
		connection.setReportDefaultSchemaAsNull(original);
	}

	@Test
	public void testGetColumnType() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			assertEquals(getSqlType(TEST_COLUMNS.get(i - 1).type), subject.getColumnType(i));
		}
	}

	private int getSqlType(Type type)
	{
		if (type == Type.bool())
			return Types.BOOLEAN;
		if (type == Type.bytes())
			return Types.BINARY;
		if (type == Type.date())
			return Types.DATE;
		if (type == Type.float64())
			return Types.DOUBLE;
		if (type == Type.int64())
			return Types.BIGINT;
		if (type == Type.string())
			return Types.NVARCHAR;
		if (type == Type.timestamp())
			return Types.TIMESTAMP;
		if (type.getCode() == Code.ARRAY)
			return Types.ARRAY;
		return Types.OTHER;
	}

	@Test
	public void getColumnTypeName() throws SQLException
	{
		int index = 1;
		for (TestColumn col : TEST_COLUMNS)
		{
			assertEquals(col.type.getCode().name(), subject.getColumnTypeName(index));
			index++;
		}
	}

	@Test
	public void testIsReadOnly() throws SQLException
	{
		assertTrue(subject.isReadOnly(1));
		assertTrue(subject.isReadOnly(TEST_COLUMNS.size()));
		for (int i = 2; i < TEST_COLUMNS.size(); i++)
			assertFalse(subject.isReadOnly(i));
	}

	@Test
	public void testIsWritable() throws SQLException
	{
		assertFalse(subject.isWritable(1));
		assertFalse(subject.isWritable(TEST_COLUMNS.size()));
		for (int i = 2; i < TEST_COLUMNS.size(); i++)
			assertTrue(subject.isWritable(i));
	}

	@Test
	public void testIsDefinitelyWritable() throws SQLException
	{
		assertFalse(subject.isDefinitelyWritable(1));
		assertFalse(subject.isDefinitelyWritable(TEST_COLUMNS.size()));
		for (int i = 2; i < TEST_COLUMNS.size(); i++)
			assertTrue(subject.isDefinitelyWritable(i));
	}

	@Test
	public void testGetColumnClassName() throws SQLException
	{
		for (int i = 1; i <= TEST_COLUMNS.size(); i++)
		{
			assertEquals(getTypeClassName(TEST_COLUMNS.get(i - 1).type), subject.getColumnClassName(i));
		}
	}

	private String getTypeClassName(Type type)
	{
		if (type == Type.bool())
			return Boolean.class.getName();
		if (type == Type.bytes())
			return Byte[].class.getName();
		if (type == Type.date())
			return Date.class.getName();
		if (type == Type.float64())
			return Double.class.getName();
		if (type == Type.int64())
			return Long.class.getName();
		if (type == Type.string())
			return String.class.getName();
		if (type == Type.timestamp())
			return Timestamp.class.getName();
		if (type.getCode() == Code.ARRAY)
		{
			if (type.getArrayElementType() == Type.bool())
				return Boolean[].class.getName();
			if (type.getArrayElementType() == Type.bytes())
				return Byte[][].class.getName();
			if (type.getArrayElementType() == Type.date())
				return Date[].class.getName();
			if (type.getArrayElementType() == Type.float64())
				return Double[].class.getName();
			if (type.getArrayElementType() == Type.int64())
				return Long[].class.getName();
			if (type.getArrayElementType() == Type.string())
				return String[].class.getName();
			if (type.getArrayElementType() == Type.timestamp())
				return Timestamp[].class.getName();
		}
		return null;
	}

	@Test
	public void testToString()
	{
		assertNotNull(subject.toString());
	}

}
