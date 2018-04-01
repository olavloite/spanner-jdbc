package nl.topicus.jdbc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;

import nl.topicus.jdbc.resultset.CloudSpannerResultSet;
import nl.topicus.jdbc.statement.CloudSpannerStatement;
import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerDataTypeTest
{

	@Test
	public void testGetType()
	{
		assertEquals(CloudSpannerDataType.INT64, CloudSpannerDataType.getType(Byte.class));
		assertEquals(CloudSpannerDataType.INT64, CloudSpannerDataType.getType(Integer.class));
		assertEquals(CloudSpannerDataType.INT64, CloudSpannerDataType.getType(Long.class));
		assertEquals(CloudSpannerDataType.FLOAT64, CloudSpannerDataType.getType(Float.class));
		assertEquals(CloudSpannerDataType.FLOAT64, CloudSpannerDataType.getType(Double.class));
		assertEquals(CloudSpannerDataType.FLOAT64, CloudSpannerDataType.getType(BigDecimal.class));
		assertEquals(CloudSpannerDataType.BOOL, CloudSpannerDataType.getType(Boolean.class));
		assertEquals(CloudSpannerDataType.BYTES, CloudSpannerDataType.getType(byte[].class));
		assertEquals(CloudSpannerDataType.DATE, CloudSpannerDataType.getType(Date.class));
		assertEquals(CloudSpannerDataType.TIMESTAMP, CloudSpannerDataType.getType(Timestamp.class));
		assertEquals(CloudSpannerDataType.STRING, CloudSpannerDataType.getType(String.class));
	}

	@Test
	public void testGetTypeByCode()
	{
		for (Code code : Code.values())
		{
			if (code != Code.ARRAY && code != Code.STRUCT)
			{
				assertEquals(code.name(), CloudSpannerDataType.getType(code).name());
			}
		}
	}

	@Test
	public void testJavaClassInSupportedClasses()
	{
		for (CloudSpannerDataType type : CloudSpannerDataType.values())
		{
			Assert.assertTrue(type.getSupportedJavaClasses().contains(type.getJavaClass()));
		}
	}

	@Test
	public void testGetArrayElements() throws SQLException
	{
		ResultSet googleResultSet = Mockito.mock(ResultSet.class);
		when(googleResultSet.getBooleanList(0)).thenReturn(Arrays.asList(true, true, false));
		when(googleResultSet.getBytesList(0))
				.thenReturn(Arrays.asList(ByteArray.copyFrom("foo"), ByteArray.copyFrom("bar")));
		when(googleResultSet.getDateList(0))
				.thenReturn(Arrays.asList(com.google.cloud.Date.fromYearMonthDay(2017, 10, 1),
						com.google.cloud.Date.fromYearMonthDay(2017, 9, 1)));
		when(googleResultSet.getDoubleList(0)).thenReturn(Arrays.asList(1d, 2d, 3d));
		when(googleResultSet.getLongList(0)).thenReturn(Arrays.asList(1l, 2l, 3l));
		when(googleResultSet.getStringList(0)).thenReturn(Arrays.asList("foo", "bar"));
		when(googleResultSet.getTimestampList(0))
				.thenReturn(Arrays.asList(com.google.cloud.Timestamp.now(), com.google.cloud.Timestamp.now()));
		when(googleResultSet.next()).thenReturn(true);

		try (CloudSpannerResultSet rs = new CloudSpannerResultSet(Mockito.mock(CloudSpannerStatement.class),
				googleResultSet, "SELECT * FROM FOO"))
		{
			rs.next();
			for (CloudSpannerDataType type : CloudSpannerDataType.values())
			{
				when(googleResultSet.getColumnType(0)).thenReturn(Type.array(type.getGoogleType()));
				Array array = rs.getArray(1);
				assertTrue(array.getArray().getClass().isArray());
				assertArrayEquals((Object[]) array.getArray(), (Object[]) ((Array) rs.getObject(1)).getArray());
			}
		}
	}

}
