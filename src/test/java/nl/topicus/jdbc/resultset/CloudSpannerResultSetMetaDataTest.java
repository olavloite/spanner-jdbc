package nl.topicus.jdbc.resultset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import org.junit.Test;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;

public class CloudSpannerResultSetMetaDataTest
{
	private CloudSpannerResultSetMetaData subject;

	private ResultSet resultSet;

	public CloudSpannerResultSetMetaDataTest()
	{
		resultSet = CloudSpannerResultSetTest.getMockResultSet();
		when(resultSet.getColumnCount()).thenReturn(4);

		resultSet.next();
		subject = new CloudSpannerResultSetMetaData(resultSet);
	}

	@Test
	public void testGetColumnCount() throws SQLException
	{
		assertEquals(resultSet.getColumnCount(), subject.getColumnCount());
	}

	@Test
	public void testIsAutoIncrement() throws SQLException
	{
		assertEquals(false, subject.isAutoIncrement(1));
	}

	@Test
	public void testIsCaseSensitive() throws SQLException
	{
		assertEquals(false, subject.isCaseSensitive(1));
	}

	@Test
	public void testIsSearchable() throws SQLException
	{
		assertTrue(subject.isSearchable(1));
	}

	@Test
	public void testIsCurrency() throws SQLException
	{
		assertEquals(false, subject.isCurrency(1));
	}

	@Test
	public void testIsNullable() throws SQLException
	{
		assertEquals(ResultSetMetaData.columnNullableUnknown, subject.isNullable(1));
	}

	@Test
	public void testIsSigned() throws SQLException
	{
		assertTrue(subject.isSigned(CloudSpannerResultSetTest.DOUBLE_COLINDEX_NOTNULL));
		assertTrue(subject.isSigned(CloudSpannerResultSetTest.LONG_COLINDEX_NULL));
		assertEquals(false, subject.isSigned(CloudSpannerResultSetTest.DATE_COLINDEX_NOTNULL));
		assertEquals(false, subject.isSigned(CloudSpannerResultSetTest.STRING_COLINDEX_NOTNULL));
	}

	@Test
	public void testGetColumnDisplaySize() throws SQLException
	{
		assertEquals(5, subject.getColumnDisplaySize(CloudSpannerResultSetTest.BOOLEAN_COLINDEX_NOTNULL));
		assertEquals(10, subject.getColumnDisplaySize(CloudSpannerResultSetTest.DATE_COLINDEX_NOTNULL));
		assertEquals(14, subject.getColumnDisplaySize(CloudSpannerResultSetTest.DOUBLE_COLINDEX_NOTNULL));
		assertEquals(10, subject.getColumnDisplaySize(CloudSpannerResultSetTest.LONG_COLINDEX_NOTNULL));
		assertEquals(50, subject.getColumnDisplaySize(CloudSpannerResultSetTest.STRING_COLINDEX_NOTNULL));
		assertEquals(16, subject.getColumnDisplaySize(CloudSpannerResultSetTest.TIMESTAMP_COLINDEX_NOTNULL));
	}

	@Test
	public void testGetColumnLabel() throws SQLException
	{
		assertEquals(CloudSpannerResultSetTest.BOOLEAN_COL_NOT_NULL,
				subject.getColumnLabel(CloudSpannerResultSetTest.BOOLEAN_COLINDEX_NOTNULL));
		assertEquals(CloudSpannerResultSetTest.DATE_COL_NOT_NULL,
				subject.getColumnLabel(CloudSpannerResultSetTest.DATE_COLINDEX_NOTNULL));
	}

	@Test
	public void testGetColumnName() throws SQLException
	{
		assertEquals(CloudSpannerResultSetTest.BOOLEAN_COL_NOT_NULL,
				subject.getColumnLabel(CloudSpannerResultSetTest.BOOLEAN_COLINDEX_NOTNULL));
		assertEquals(CloudSpannerResultSetTest.DATE_COL_NOT_NULL,
				subject.getColumnLabel(CloudSpannerResultSetTest.DATE_COLINDEX_NOTNULL));
	}

	@Test
	public void testGetSchemaName() throws SQLException
	{
		assertEquals("", subject.getSchemaName(1));
	}

	@Test
	public void testGetPrecision() throws SQLException
	{
		assertEquals(0, subject.getPrecision(1));
	}

	@Test
	public void testGetScale() throws SQLException
	{
		assertEquals(0, subject.getScale(1));
	}

	@Test
	public void testGetTableName() throws SQLException
	{
		assertEquals("", subject.getTableName(1));
	}

	@Test
	public void testGetCatalogName() throws SQLException
	{
		assertEquals("", subject.getCatalogName(1));
	}

	@Test
	public void testGetColumnType() throws SQLException
	{
		assertEquals(Types.BIGINT, subject.getColumnType(CloudSpannerResultSetTest.LONG_COLINDEX_NOTNULL));
		assertEquals(Types.BOOLEAN, subject.getColumnType(CloudSpannerResultSetTest.BOOLEAN_COLINDEX_NOTNULL));
		assertEquals(Types.DATE, subject.getColumnType(CloudSpannerResultSetTest.DATE_COLINDEX_NOTNULL));
		assertEquals(Types.DOUBLE, subject.getColumnType(CloudSpannerResultSetTest.DOUBLE_COLINDEX_NOTNULL));
		assertEquals(Types.NVARCHAR, subject.getColumnType(CloudSpannerResultSetTest.STRING_COLINDEX_NOTNULL));
		assertEquals(Types.TIMESTAMP, subject.getColumnType(CloudSpannerResultSetTest.TIMESTAMP_COLINDEX_NOTNULL));
	}

	@Test
	public void getColumnTypeName() throws SQLException
	{
		assertEquals(Type.Code.INT64.name(),
				subject.getColumnTypeName(CloudSpannerResultSetTest.LONG_COLINDEX_NOTNULL));
		assertEquals(Type.Code.BOOL.name(),
				subject.getColumnTypeName(CloudSpannerResultSetTest.BOOLEAN_COLINDEX_NOTNULL));
		assertEquals(Type.Code.DATE.name(), subject.getColumnTypeName(CloudSpannerResultSetTest.DATE_COLINDEX_NOTNULL));
		assertEquals(Type.Code.FLOAT64.name(),
				subject.getColumnTypeName(CloudSpannerResultSetTest.DOUBLE_COLINDEX_NOTNULL));
		assertEquals(Type.Code.STRING.name(),
				subject.getColumnTypeName(CloudSpannerResultSetTest.STRING_COLINDEX_NOTNULL));
		assertEquals(Type.Code.TIMESTAMP.name(),
				subject.getColumnTypeName(CloudSpannerResultSetTest.TIMESTAMP_COLINDEX_NOTNULL));
	}

	@Test
	public void testIsReadOnly() throws SQLException
	{
		assertEquals(false, subject.isReadOnly(1));
	}

	@Test
	public void testIsWritable() throws SQLException
	{
		assertEquals(true, subject.isWritable(1));
	}

	@Test
	public void testIsDefinitelyWritable() throws SQLException
	{
		assertEquals(false, subject.isDefinitelyWritable(1));
	}

	@Test
	public void testGetColumnClassName() throws SQLException
	{
		assertEquals(Long.class.getName(), subject.getColumnClassName(CloudSpannerResultSetTest.LONG_COLINDEX_NOTNULL));
		assertEquals(Boolean.class.getName(),
				subject.getColumnClassName(CloudSpannerResultSetTest.BOOLEAN_COLINDEX_NOTNULL));
		assertEquals(Date.class.getName(), subject.getColumnClassName(CloudSpannerResultSetTest.DATE_COLINDEX_NOTNULL));
		assertEquals(Double.class.getName(),
				subject.getColumnClassName(CloudSpannerResultSetTest.DOUBLE_COLINDEX_NOTNULL));
		assertEquals(String.class.getName(),
				subject.getColumnClassName(CloudSpannerResultSetTest.STRING_COLINDEX_NOTNULL));
		assertEquals(Timestamp.class.getName(),
				subject.getColumnClassName(CloudSpannerResultSetTest.TIMESTAMP_COLINDEX_NOTNULL));
	}

	@Test
	public void testToString()
	{
		assertNotNull(subject.toString());
	}

}
