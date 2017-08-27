package nl.topicus.jdbc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerArrayTest
{
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private final String[] inputArray = new String[] { "one", "two", "three" };

	private final List<CloudSpannerArray> testSubjects = new ArrayList<>();

	public CloudSpannerArrayTest() throws SQLException
	{
		testSubjects.add(CloudSpannerArray.createArray("String", inputArray));
		testSubjects.add(CloudSpannerArray.createArray(CloudSpannerDataType.STRING, Arrays.asList(inputArray)));
	}

	@Test
	public void testGetBaseTypeName() throws SQLException
	{
		for (CloudSpannerArray testSubject : testSubjects)
			assertEquals("STRING", testSubject.getBaseTypeName());
	}

	@Test
	public void testGetType() throws SQLException
	{
		for (CloudSpannerArray testSubject : testSubjects)
			assertEquals(Types.NVARCHAR, testSubject.getBaseType());
	}

	@Test
	public void testGetArray() throws SQLException
	{
		for (CloudSpannerArray testSubject : testSubjects)
		{
			assertArrayEquals(inputArray, (Object[]) testSubject.getArray());
			assertArrayEquals(inputArray, (Object[]) testSubject.getArray(Collections.emptyMap()));
		}
	}

	@Test
	public void testGetResultSet() throws SQLException
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		testSubjects.get(0).getResultSet();
	}

	@Test
	public void testGetResultSetWithIndex() throws SQLException
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		testSubjects.get(0).getResultSet(0, 0);
	}

	@Test
	public void testGetResultSetWithMap() throws SQLException
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		testSubjects.get(0).getResultSet(null);
	}

	@Test
	public void testGetResultSetWithMapAndIndex() throws SQLException
	{
		thrown.expect(SQLFeatureNotSupportedException.class);
		testSubjects.get(0).getResultSet(0, 0, null);
	}

}
