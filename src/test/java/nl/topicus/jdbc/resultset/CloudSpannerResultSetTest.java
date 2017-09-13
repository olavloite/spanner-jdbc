package nl.topicus.jdbc.resultset;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerResultSetTest
{
	private static final String STRING_COL_NULL = "STRING_COL_NULL";

	private static final String STRING_COL_NOT_NULL = "STRING_COL_NOT_NULL";

	private static final int STRING_COLINDEX_NULL = 1;

	private static final int STRING_COLINDEX_NOTNULL = 2;

	private static final String BOOLEAN_COL_NULL = "BOOLEAN_COL_NULL";

	private static final String BOOLEAN_COL_NOT_NULL = "BOOLEAN_COL_NOT_NULL";

	private static final int BOOLEAN_COLINDEX_NULL = 1;

	private static final int BOOLEAN_COLINDEX_NOTNULL = 2;

	private static final String DOUBLE_COL_NULL = "DOUBLE_COL_NULL";

	private static final String DOUBLE_COL_NOT_NULL = "DOUBLE_COL_NOT_NULL";

	private static final int DOUBLE_COLINDEX_NULL = 1;

	private static final int DOUBLE_COLINDEX_NOTNULL = 2;

	private static final String BYTES_COL_NULL = "BYTES_COL_NULL";

	private static final String BYTES_COL_NOT_NULL = "BYTES_COL_NOT_NULL";

	private static final int BYTES_COLINDEX_NULL = 1;

	private static final int BYTES_COLINDEX_NOTNULL = 2;

	private static final String LONG_COL_NULL = "LONG_COL_NULL";

	private static final String LONG_COL_NOT_NULL = "LONG_COL_NOT_NULL";

	private static final int LONG_COLINDEX_NULL = 1;

	private static final int LONG_COLINDEX_NOTNULL = 2;

	private static final String DATE_COL_NULL = "DATE_COL_NULL";

	private static final String DATE_COL_NOT_NULL = "DATE_COL_NOT_NULL";

	private static final int DATE_COLINDEX_NULL = 1;

	private static final int DATE_COLINDEX_NOTNULL = 2;

	private static final String TIMESTAMP_COL_NULL = "TIMESTAMP_COL_NULL";

	private static final String TIMESTAMP_COL_NOT_NULL = "TIMESTAMP_COL_NOT_NULL";

	private static final int TIMESTAMP_COLINDEX_NULL = 1;

	private static final int TIMESTAMP_COLINDEX_NOTNULL = 2;

	private static final String TIME_COL_NULL = "TIME_COL_NULL";

	private static final String TIME_COL_NOT_NULL = "TIME_COL_NOT_NULL";

	private static final int TIME_COLINDEX_NULL = 3;

	private static final int TIME_COLINDEX_NOTNULL = 4;

	private CloudSpannerResultSet subject;

	private static ResultSet getMockResultSet()
	{
		ResultSet res = mock(ResultSet.class);
		when(res.getString(STRING_COL_NULL)).thenReturn(null);
		when(res.isNull(STRING_COL_NULL)).thenReturn(true);
		when(res.getString(STRING_COL_NOT_NULL)).thenReturn("FOO");
		when(res.isNull(STRING_COL_NOT_NULL)).thenReturn(false);
		when(res.getString(STRING_COLINDEX_NULL - 1)).thenReturn(null);
		when(res.isNull(STRING_COLINDEX_NULL - 1)).thenReturn(true);
		when(res.getString(STRING_COLINDEX_NOTNULL - 1)).thenReturn("BAR");
		when(res.isNull(STRING_COLINDEX_NOTNULL - 1)).thenReturn(false);

		when(res.getBoolean(BOOLEAN_COL_NULL)).thenReturn(false);
		when(res.isNull(BOOLEAN_COL_NULL)).thenReturn(true);
		when(res.getBoolean(BOOLEAN_COL_NOT_NULL)).thenReturn(true);
		when(res.isNull(BOOLEAN_COL_NOT_NULL)).thenReturn(false);
		when(res.getBoolean(BOOLEAN_COLINDEX_NULL - 1)).thenReturn(false);
		when(res.isNull(BOOLEAN_COLINDEX_NULL - 1)).thenReturn(true);
		when(res.getBoolean(BOOLEAN_COLINDEX_NOTNULL - 1)).thenReturn(false);
		when(res.isNull(BOOLEAN_COLINDEX_NOTNULL - 1)).thenReturn(false);

		when(res.getDouble(DOUBLE_COL_NULL)).thenReturn(0d);
		when(res.isNull(DOUBLE_COL_NULL)).thenReturn(true);
		when(res.getDouble(DOUBLE_COL_NOT_NULL)).thenReturn(1.123456789d);
		when(res.isNull(DOUBLE_COL_NOT_NULL)).thenReturn(false);
		when(res.getDouble(DOUBLE_COLINDEX_NULL - 1)).thenReturn(0d);
		when(res.isNull(DOUBLE_COLINDEX_NULL - 1)).thenReturn(true);
		when(res.getDouble(DOUBLE_COLINDEX_NOTNULL - 1)).thenReturn(2.123456789d);
		when(res.isNull(DOUBLE_COLINDEX_NOTNULL - 1)).thenReturn(false);

		when(res.getString(BYTES_COL_NULL)).thenReturn(null);
		when(res.isNull(BYTES_COL_NULL)).thenReturn(true);
		when(res.getBytes(BYTES_COL_NOT_NULL)).thenReturn(ByteArray.copyFrom("FOO"));
		when(res.isNull(BYTES_COL_NOT_NULL)).thenReturn(false);
		when(res.getBytes(BYTES_COLINDEX_NULL - 1)).thenReturn(null);
		when(res.isNull(BYTES_COLINDEX_NULL - 1)).thenReturn(true);
		when(res.getBytes(BYTES_COLINDEX_NOTNULL - 1)).thenReturn(ByteArray.copyFrom("BAR"));
		when(res.isNull(BYTES_COLINDEX_NOTNULL - 1)).thenReturn(false);

		when(res.getLong(LONG_COL_NULL)).thenReturn(0l);
		when(res.isNull(LONG_COL_NULL)).thenReturn(true);
		when(res.getLong(LONG_COL_NOT_NULL)).thenReturn(1l);
		when(res.isNull(LONG_COL_NOT_NULL)).thenReturn(false);
		when(res.getLong(LONG_COLINDEX_NULL - 1)).thenReturn(0l);
		when(res.isNull(LONG_COLINDEX_NULL - 1)).thenReturn(true);
		when(res.getLong(LONG_COLINDEX_NOTNULL - 1)).thenReturn(2l);
		when(res.isNull(LONG_COLINDEX_NOTNULL - 1)).thenReturn(false);

		when(res.getDate(DATE_COL_NULL)).thenReturn(null);
		when(res.isNull(DATE_COL_NULL)).thenReturn(true);
		when(res.getDate(DATE_COL_NOT_NULL)).thenReturn(Date.fromYearMonthDay(2017, 9, 10));
		when(res.isNull(DATE_COL_NOT_NULL)).thenReturn(false);
		when(res.getDate(DATE_COLINDEX_NULL - 1)).thenReturn(null);
		when(res.isNull(DATE_COLINDEX_NULL - 1)).thenReturn(true);
		when(res.getDate(DATE_COLINDEX_NOTNULL - 1)).thenReturn(Date.fromYearMonthDay(2017, 9, 11));
		when(res.isNull(DATE_COLINDEX_NOTNULL - 1)).thenReturn(false);

		Calendar cal1 = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal1.clear();
		cal1.set(2017, 8, 10, 8, 15, 59);
		Calendar cal2 = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal2.clear();
		cal2.set(2017, 8, 11, 8, 15, 59);
		when(res.getTimestamp(TIMESTAMP_COL_NULL)).thenReturn(null);
		when(res.isNull(TIMESTAMP_COL_NULL)).thenReturn(true);
		when(res.getTimestamp(TIMESTAMP_COL_NOT_NULL)).thenReturn(Timestamp.of(cal1.getTime()));
		when(res.isNull(TIMESTAMP_COL_NOT_NULL)).thenReturn(false);
		when(res.getTimestamp(TIMESTAMP_COLINDEX_NULL - 1)).thenReturn(null);
		when(res.isNull(TIMESTAMP_COLINDEX_NULL - 1)).thenReturn(true);
		when(res.getTimestamp(TIMESTAMP_COLINDEX_NOTNULL - 1)).thenReturn(Timestamp.of(cal2.getTime()));
		when(res.isNull(TIMESTAMP_COLINDEX_NOTNULL - 1)).thenReturn(false);

		Calendar cal3 = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal3.clear();
		cal3.set(1970, 0, 1, 14, 6, 15);
		Calendar cal4 = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal4.clear();
		cal4.set(1970, 0, 1, 14, 6, 15);
		when(res.getTimestamp(TIME_COL_NULL)).thenReturn(null);
		when(res.isNull(TIME_COL_NULL)).thenReturn(true);
		when(res.getTimestamp(TIME_COL_NOT_NULL)).thenReturn(Timestamp.of(cal3.getTime()));
		when(res.isNull(TIME_COL_NOT_NULL)).thenReturn(false);
		when(res.getTimestamp(TIME_COLINDEX_NULL - 1)).thenReturn(null);
		when(res.isNull(TIME_COLINDEX_NULL - 1)).thenReturn(true);
		when(res.getTimestamp(TIME_COLINDEX_NOTNULL - 1)).thenReturn(Timestamp.of(cal4.getTime()));
		when(res.isNull(TIME_COLINDEX_NOTNULL - 1)).thenReturn(false);

		when(res.getColumnIndex(STRING_COL_NOT_NULL)).thenReturn(1);

		// Next behaviour.
		when(res.next()).thenReturn(true, true, true, true, false);

		return res;
	}

	public CloudSpannerResultSetTest() throws SQLException
	{
		subject = new CloudSpannerResultSet(getMockResultSet());
		subject.next();
	}

	@Test
	public void testWasNull() throws SQLException
	{
		String value = subject.getString(STRING_COL_NULL);
		boolean wasNull = subject.wasNull();
		assertTrue(wasNull);
		assertNull(value);
		String valueNotNull = subject.getString(STRING_COL_NOT_NULL);
		boolean wasNotNull = subject.wasNull();
		assertEquals(false, wasNotNull);
		assertNotNull(valueNotNull);
	}

	@Test
	public void testNext() throws SQLException
	{
		try (CloudSpannerResultSet rs = new CloudSpannerResultSet(getMockResultSet()))
		{
			assertTrue(rs.isBeforeFirst());
			assertEquals(false, rs.isAfterLast());
			int num = 0;
			while (rs.next())
			{
				num++;
			}
			assertTrue(num > 0);
			assertEquals(false, rs.isBeforeFirst());
			assertTrue(rs.isAfterLast());
		}
	}

	@Test
	public void testClose() throws SQLException
	{
		try (CloudSpannerResultSet rs = new CloudSpannerResultSet(getMockResultSet()))
		{
			assertEquals(false, rs.isClosed());
			rs.next();
			assertNotNull(rs.getString(STRING_COL_NOT_NULL));
			rs.close();
			assertTrue(rs.isClosed());
			boolean failed = false;
			try
			{
				// Should fail
				rs.getString(STRING_COL_NOT_NULL);
			}
			catch (SQLException e)
			{
				failed = true;
			}
			assertTrue(failed);
		}
	}

	@Test
	public void testGetStringIndex() throws SQLException
	{
		assertNotNull(subject.getString(STRING_COLINDEX_NOTNULL));
		assertEquals("BAR", subject.getString(STRING_COLINDEX_NOTNULL));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getString(STRING_COLINDEX_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetBooleanIndex() throws SQLException
	{
		assertNotNull(subject.getBoolean(BOOLEAN_COLINDEX_NOTNULL));
		assertEquals(false, subject.wasNull());
		assertEquals(false, subject.getBoolean(BOOLEAN_COLINDEX_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetLongIndex() throws SQLException
	{
		assertNotNull(subject.getLong(LONG_COLINDEX_NOTNULL));
		assertEquals(2l, subject.getLong(LONG_COLINDEX_NOTNULL));
		assertEquals(false, subject.wasNull());
		assertEquals(0l, subject.getLong(LONG_COLINDEX_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetDoubleIndex() throws SQLException
	{
		assertNotNull(subject.getDouble(DOUBLE_COLINDEX_NOTNULL));
		assertEquals(2.123456789d, subject.getDouble(DOUBLE_COLINDEX_NOTNULL), 0d);
		assertEquals(false, subject.wasNull());
		assertEquals(0d, subject.getDouble(DOUBLE_COLINDEX_NULL), 0d);
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetBigDecimalIndexAndScale() throws SQLException
	{
		assertNotNull(subject.getBigDecimal(DOUBLE_COLINDEX_NOTNULL, 2));
		assertEquals(BigDecimal.valueOf(2.12d), subject.getBigDecimal(DOUBLE_COLINDEX_NOTNULL, 2));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getBigDecimal(DOUBLE_COLINDEX_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetBytesIndex() throws SQLException
	{
		assertNotNull(subject.getBytes(BYTES_COLINDEX_NOTNULL));
		assertArrayEquals(ByteArray.copyFrom("BAR").toByteArray(), subject.getBytes(BYTES_COLINDEX_NOTNULL));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getBytes(BYTES_COLINDEX_NULL));
		assertTrue(subject.wasNull());
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testGetDateIndex() throws SQLException
	{
		assertNotNull(subject.getDate(DATE_COLINDEX_NOTNULL));
		assertEquals(new java.sql.Date(2017 - 1900, 8, 11), subject.getDate(DATE_COLINDEX_NOTNULL));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getDate(DATE_COLINDEX_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetTimeIndex() throws SQLException
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal.clear();
		cal.set(1970, 0, 1, 14, 6, 15);

		assertNotNull(subject.getTime(TIME_COLINDEX_NOTNULL));
		assertEquals(new Time(cal.getTimeInMillis()), subject.getTime(TIME_COLINDEX_NOTNULL));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getTime(TIME_COLINDEX_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetTimestampIndex() throws SQLException
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal.clear();
		cal.set(2017, 8, 11, 8, 15, 59);

		assertNotNull(subject.getTime(TIMESTAMP_COLINDEX_NOTNULL));
		assertEquals(new java.sql.Timestamp(cal.getTimeInMillis()), subject.getTimestamp(TIMESTAMP_COLINDEX_NOTNULL));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getTimestamp(TIMESTAMP_COLINDEX_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetStringLabel() throws SQLException
	{
		assertNotNull(subject.getString(STRING_COL_NOT_NULL));
		assertEquals("FOO", subject.getString(STRING_COL_NOT_NULL));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getString(STRING_COL_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetBooleanLabel() throws SQLException
	{
		assertNotNull(subject.getBoolean(BOOLEAN_COL_NOT_NULL));
		assertEquals(false, subject.wasNull());
		assertEquals(false, subject.getBoolean(BOOLEAN_COL_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetLongLabel() throws SQLException
	{
		assertNotNull(subject.getLong(LONG_COL_NOT_NULL));
		assertEquals(1l, subject.getLong(LONG_COL_NOT_NULL));
		assertEquals(false, subject.wasNull());
		assertEquals(0l, subject.getLong(LONG_COL_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetDoubleLabel() throws SQLException
	{
		assertNotNull(subject.getDouble(DOUBLE_COL_NOT_NULL));
		assertEquals(1.123456789d, subject.getDouble(DOUBLE_COL_NOT_NULL), 0d);
		assertEquals(false, subject.wasNull());
		assertEquals(0d, subject.getDouble(DOUBLE_COL_NULL), 0d);
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetBigDecimalLabelAndScale() throws SQLException
	{
		assertNotNull(subject.getBigDecimal(DOUBLE_COL_NOT_NULL, 2));
		assertEquals(BigDecimal.valueOf(1.12d), subject.getBigDecimal(DOUBLE_COL_NOT_NULL, 2));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getBigDecimal(DOUBLE_COL_NULL, 2));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetBytesLabel() throws SQLException
	{
		assertNotNull(subject.getBytes(BYTES_COL_NOT_NULL));
		assertArrayEquals(ByteArray.copyFrom("FOO").toByteArray(), subject.getBytes(BYTES_COL_NOT_NULL));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getBytes(BYTES_COL_NULL));
		assertTrue(subject.wasNull());
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testGetDateLabel() throws SQLException
	{
		assertNotNull(subject.getDate(DATE_COL_NOT_NULL));
		assertEquals(new java.sql.Date(2017 - 1900, 8, 10), subject.getDate(DATE_COL_NOT_NULL));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getDate(DATE_COL_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetTimeLabel() throws SQLException
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal.clear();
		cal.set(1970, 0, 1, 14, 6, 15);

		assertNotNull(subject.getTime(TIME_COL_NOT_NULL));
		assertEquals(new Time(cal.getTimeInMillis()), subject.getTime(TIME_COL_NOT_NULL));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getTime(TIME_COL_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetTimestampLabel() throws SQLException
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal.clear();
		cal.set(2017, 8, 10, 8, 15, 59);

		assertNotNull(subject.getTime(TIMESTAMP_COL_NOT_NULL));
		assertEquals(new java.sql.Timestamp(cal.getTimeInMillis()), subject.getTimestamp(TIMESTAMP_COL_NOT_NULL));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getTimestamp(TIMESTAMP_COL_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetMetaData() throws SQLException
	{
		CloudSpannerResultSetMetaData metadata = subject.getMetaData();
		assertNotNull(metadata);
	}

	@Test
	public void testFindColumn() throws SQLException
	{
		assertEquals(2, subject.findColumn(STRING_COL_NOT_NULL));
	}

	@Test
	public void testGetBigDecimalIndex() throws SQLException
	{
		assertNotNull(subject.getBigDecimal(DOUBLE_COLINDEX_NOTNULL));
		assertEquals(BigDecimal.valueOf(2.123456789d), subject.getBigDecimal(DOUBLE_COLINDEX_NOTNULL));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getBigDecimal(DOUBLE_COLINDEX_NULL));
		assertTrue(subject.wasNull());
	}

	@Test
	public void testGetBigDecimalLabel() throws SQLException
	{
		assertNotNull(subject.getBigDecimal(DOUBLE_COL_NOT_NULL));
		assertEquals(BigDecimal.valueOf(1.123456789d), subject.getBigDecimal(DOUBLE_COL_NOT_NULL));
		assertEquals(false, subject.wasNull());
		assertNull(subject.getBigDecimal(DOUBLE_COL_NULL));
		assertTrue(subject.wasNull());
	}
	//
	// @Test
	// public Statement getStatement() throws SQLException
	// {
	// ensureOpen();
	// return statement;
	// }
	//
	// @Test
	// public Date getDate(int columnIndex, Calendar cal) throws SQLException
	// {
	// return isNull(columnIndex) ? null :
	// CloudSpannerConversionUtil.toSqlDate(resultSet.getDate(columnIndex - 1));
	// }
	//
	// @Test
	// public Date getDate(String columnLabel, Calendar cal) throws SQLException
	// {
	// return isNull(columnLabel) ? null :
	// CloudSpannerConversionUtil.toSqlDate(resultSet.getDate(columnLabel));
	// }
	//
	// @Test
	// public Time getTime(int columnIndex, Calendar cal) throws SQLException
	// {
	// return isNull(columnIndex) ? null :
	// toTime(resultSet.getTimestamp(columnIndex - 1));
	// }
	//
	// @Test
	// public Time getTime(String columnLabel, Calendar cal) throws SQLException
	// {
	// return isNull(columnLabel) ? null :
	// toTime(resultSet.getTimestamp(columnLabel));
	// }
	//
	// @Test
	// public Timestamp getTimestamp(int columnIndex, Calendar cal) throws
	// SQLException
	// {
	// return isNull(columnIndex) ? null
	// :
	// CloudSpannerConversionUtil.toSqlTimestamp(resultSet.getTimestamp(columnIndex
	// - 1));
	// }
	//
	// @Test
	// public Timestamp getTimestamp(String columnLabel, Calendar cal) throws
	// SQLException
	// {
	// return isNull(columnLabel) ? null
	// :
	// CloudSpannerConversionUtil.toSqlTimestamp(resultSet.getTimestamp(columnLabel));
	// }
	//
	// @Test
	// public boolean isClosed() throws SQLException
	// {
	// return closed;
	// }
	//
	// @Test
	// public byte getByte(int columnIndex) throws SQLException
	// {
	// return isNull(columnIndex) ? 0 : (byte) resultSet.getLong(columnIndex -
	// 1);
	// }
	//
	// @Test
	// public short getShort(int columnIndex) throws SQLException
	// {
	// return isNull(columnIndex) ? 0 : (short) resultSet.getLong(columnIndex -
	// 1);
	// }
	//
	// @Test
	// public int getInt(int columnIndex) throws SQLException
	// {
	// return isNull(columnIndex) ? 0 : (int) resultSet.getLong(columnIndex -
	// 1);
	// }
	//
	// @Test
	// public float getFloat(int columnIndex) throws SQLException
	// {
	// return isNull(columnIndex) ? 0 : (float) resultSet.getDouble(columnIndex
	// - 1);
	// }
	//
	// @Test
	// public byte getByte(String columnLabel) throws SQLException
	// {
	// return isNull(columnLabel) ? 0 : (byte) resultSet.getLong(columnLabel);
	// }
	//
	// @Test
	// public short getShort(String columnLabel) throws SQLException
	// {
	// return isNull(columnLabel) ? 0 : (short) resultSet.getLong(columnLabel);
	// }
	//
	// @Test
	// public int getInt(String columnLabel) throws SQLException
	// {
	// return isNull(columnLabel) ? 0 : (int) resultSet.getLong(columnLabel);
	// }
	//
	// @Test
	// public float getFloat(String columnLabel) throws SQLException
	// {
	// return isNull(columnLabel) ? 0 : (float)
	// resultSet.getDouble(columnLabel);
	// }
	//
	// @Test
	// public Object getObject(String columnLabel) throws SQLException
	// {
	// Type type = resultSet.getColumnType(columnLabel);
	// return isNull(columnLabel) ? null : getObject(type, columnLabel);
	// }
	//
	// @Test
	// public Object getObject(int columnIndex) throws SQLException
	// {
	// Type type = resultSet.getColumnType(columnIndex - 1);
	// return isNull(columnIndex) ? null : getObject(type, columnIndex);
	// }
	//
	// private Object getObject(Type type, String columnLabel) throws
	// SQLException
	// {
	// return getObject(type, resultSet.getColumnIndex(columnLabel) + 1);
	// }
	//
	// private Object getObject(Type type, int columnIndex) throws SQLException
	// {
	// if (type == Type.bool())
	// return getBoolean(columnIndex);
	// if (type == Type.bytes())
	// return getBytes(columnIndex);
	// if (type == Type.date())
	// return getDate(columnIndex);
	// if (type == Type.float64())
	// return getDouble(columnIndex);
	// if (type == Type.int64())
	// return getLong(columnIndex);
	// if (type == Type.string())
	// return getString(columnIndex);
	// if (type == Type.timestamp())
	// return getTimestamp(columnIndex);
	// if (type.getCode() == Code.ARRAY)
	// return getArray(columnIndex);
	// throw new SQLException("Unknown type: " + type.toString());
	// }
	//
	// @Test
	// public Array getArray(String columnLabel) throws SQLException
	// {
	// Type type = resultSet.getColumnType(columnLabel);
	// if (type.getCode() != Code.ARRAY)
	// throw new SQLException("Column with label " + columnLabel + " does not
	// contain an array");
	// return getArray(resultSet.getColumnIndex(columnLabel) + 1);
	// }
	//
	// @Test
	// public Array getArray(int columnIndex) throws SQLException
	// {
	// if (isNull(columnIndex))
	// return null;
	// Type type = resultSet.getColumnType(columnIndex - 1);
	// if (type.getCode() != Code.ARRAY)
	// throw new SQLException("Column with index " + columnIndex + " does not
	// contain an array");
	// CloudSpannerDataType dataType =
	// CloudSpannerDataType.getType(type.getArrayElementType().getCode());
	// List<? extends Object> elements = dataType.getArrayElements(resultSet,
	// columnIndex - 1);
	//
	// return CloudSpannerArray.createArray(dataType, elements);
	// }
	//
	// @Test
	// public SQLWarning getWarnings() throws SQLException
	// {
	// ensureOpen();
	// return null;
	// }
	//
	// @Test
	// public void clearWarnings() throws SQLException
	// {
	// ensureOpen();
	// }
	//
	// @Test
	// public boolean isBeforeFirst() throws SQLException
	// {
	// ensureOpen();
	// return beforeFirst;
	// }
	//
	// @Test
	// public boolean isAfterLast() throws SQLException
	// {
	// ensureOpen();
	// return afterLast;
	// }
	//
	// @Test
	// public Reader getCharacterStream(int columnIndex) throws SQLException
	// {
	// String val = getString(columnIndex);
	// return val == null ? null : new StringReader(val);
	// }
	//
	// @Test
	// public Reader getCharacterStream(String columnLabel) throws SQLException
	// {
	// String val = getString(columnLabel);
	// return val == null ? null : new StringReader(val);
	// }
	//
	// private InputStream getInputStream(String val, Charset charset)
	// {
	// if (val == null)
	// return null;
	// byte[] b = val.getBytes(charset);
	// return new ByteArrayInputStream(b);
	// }
	//
	// @Test
	// public InputStream getAsciiStream(int columnIndex) throws SQLException
	// {
	// return getInputStream(getString(columnIndex), StandardCharsets.US_ASCII);
	// }
	//
	// @Test
	// public InputStream getUnicodeStream(int columnIndex) throws SQLException
	// {
	// return getInputStream(getString(columnIndex), StandardCharsets.UTF_16LE);
	// }
	//
	// @Test
	// public InputStream getBinaryStream(int columnIndex) throws SQLException
	// {
	// byte[] val = getBytes(columnIndex);
	// return val == null ? null : new ByteArrayInputStream(val);
	// }
	//
	// @Test
	// public InputStream getAsciiStream(String columnLabel) throws SQLException
	// {
	// return getInputStream(getString(columnLabel), StandardCharsets.US_ASCII);
	// }
	//
	// @Test
	// public InputStream getUnicodeStream(String columnLabel) throws
	// SQLException
	// {
	// return getInputStream(getString(columnLabel), StandardCharsets.UTF_16LE);
	// }
	//
	// @Test
	// public InputStream getBinaryStream(String columnLabel) throws
	// SQLException
	// {
	// byte[] val = getBytes(columnLabel);
	// return val == null ? null : new ByteArrayInputStream(val);
	// }
	//
	// @Test
	// public String getNString(int columnIndex) throws SQLException
	// {
	// return getString(columnIndex);
	// }
	//
	// @Test
	// public String getNString(String columnLabel) throws SQLException
	// {
	// return getString(columnLabel);
	// }
	//
	// @Test
	// public Reader getNCharacterStream(int columnIndex) throws SQLException
	// {
	// return getCharacterStream(columnIndex);
	// }
	//
	// @Test
	// public Reader getNCharacterStream(String columnLabel) throws SQLException
	// {
	// return getCharacterStream(columnLabel);
	// }

}
