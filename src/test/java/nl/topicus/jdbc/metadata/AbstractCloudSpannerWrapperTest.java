package nl.topicus.jdbc.metadata;

import static org.junit.Assert.assertEquals;

import java.sql.Types;

import org.junit.Test;

import com.google.cloud.spanner.Type;

public class AbstractCloudSpannerWrapperTest
{

	@Test
	public void testExtractColumnType() throws Exception
	{
		assertEquals(Types.BOOLEAN, AbstractCloudSpannerWrapper.extractColumnType(Type.bool()));
		assertEquals(Types.BINARY, AbstractCloudSpannerWrapper.extractColumnType(Type.bytes()));
		assertEquals(Types.DATE, AbstractCloudSpannerWrapper.extractColumnType(Type.date()));
		assertEquals(Types.DOUBLE, AbstractCloudSpannerWrapper.extractColumnType(Type.float64()));
		assertEquals(Types.BIGINT, AbstractCloudSpannerWrapper.extractColumnType(Type.int64()));
		assertEquals(Types.NVARCHAR, AbstractCloudSpannerWrapper.extractColumnType(Type.string()));
		assertEquals(Types.TIMESTAMP, AbstractCloudSpannerWrapper.extractColumnType(Type.timestamp()));
		assertEquals(Types.ARRAY, AbstractCloudSpannerWrapper.extractColumnType(Type.array(Type.bool())));
	}

	@Test
	public void testGetGoogleTypeName() throws Exception
	{
		assertEquals("BOOL", AbstractCloudSpannerWrapper.getGoogleTypeName(Types.BOOLEAN));
		assertEquals("BYTES", AbstractCloudSpannerWrapper.getGoogleTypeName(Types.BINARY));
		assertEquals("DATE", AbstractCloudSpannerWrapper.getGoogleTypeName(Types.DATE));
		assertEquals("FLOAT64", AbstractCloudSpannerWrapper.getGoogleTypeName(Types.DOUBLE));
		assertEquals("INT64", AbstractCloudSpannerWrapper.getGoogleTypeName(Types.BIGINT));
		assertEquals("STRING", AbstractCloudSpannerWrapper.getGoogleTypeName(Types.NVARCHAR));
		assertEquals("TIMESTAMP", AbstractCloudSpannerWrapper.getGoogleTypeName(Types.TIMESTAMP));
		assertEquals("ARRAY", AbstractCloudSpannerWrapper.getGoogleTypeName(Types.ARRAY));
	}

	@Test
	public void testGetClassName() throws Exception
	{
		assertEquals("java.lang.Boolean", AbstractCloudSpannerWrapper.getClassName(Types.BOOLEAN));
		assertEquals("[Ljava.lang.Byte;", AbstractCloudSpannerWrapper.getClassName(Types.BINARY));
		assertEquals("java.sql.Date", AbstractCloudSpannerWrapper.getClassName(Types.DATE));
		assertEquals("java.lang.Double", AbstractCloudSpannerWrapper.getClassName(Types.DOUBLE));
		assertEquals("java.lang.Long", AbstractCloudSpannerWrapper.getClassName(Types.BIGINT));
		assertEquals("java.lang.String", AbstractCloudSpannerWrapper.getClassName(Types.NVARCHAR));
		assertEquals("java.sql.Timestamp", AbstractCloudSpannerWrapper.getClassName(Types.TIMESTAMP));
		assertEquals("java.lang.Object", AbstractCloudSpannerWrapper.getClassName(Types.ARRAY));
	}
}