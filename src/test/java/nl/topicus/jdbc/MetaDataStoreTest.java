package nl.topicus.jdbc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import nl.topicus.jdbc.MetaDataStore.TableKeyMetaData;
import nl.topicus.jdbc.test.category.UnitTest;
import nl.topicus.jdbc.test.util.CloudSpannerTestObjects;

@Category(UnitTest.class)
public class MetaDataStoreTest
{
	@Test
	public void testGetTable() throws SQLException
	{
		MetaDataStore subject = new MetaDataStore(CloudSpannerTestObjects.createConnection());
		assertNull(subject.getTable(null));
		TableKeyMetaData fooUpperCase = subject.getTable("FOO");
		TableKeyMetaData fooLowerCase = subject.getTable("foo");
		assertNotNull(fooLowerCase);
		assertNotNull(fooUpperCase);
		assertEquals(fooLowerCase, fooUpperCase);
		assertArrayEquals(new String[] { "ID" }, fooUpperCase.getKeyColumns().toArray());
		assertArrayEquals(new String[] { "ID1", "ID2" }, subject.getTable("BAR").getKeyColumns().toArray());
	}

	@Test
	public void testClear() throws SQLException, NoSuchFieldException, SecurityException, IllegalArgumentException,
			IllegalAccessException
	{
		for (int i = 0; i < 2; i++)
		{
			MetaDataStore subject = new MetaDataStore(CloudSpannerTestObjects.createConnection());
			TableKeyMetaData fooUpperCase = subject.getTable("FOO");
			TableKeyMetaData fooLowerCase = subject.getTable("foo");
			TableKeyMetaData bar = subject.getTable("Bar");
			assertNotNull(fooLowerCase);
			assertNotNull(fooUpperCase);
			assertNotNull(bar);
			assertEquals(fooLowerCase, fooUpperCase);
			Assert.assertNotEquals(fooLowerCase, bar);

			Field field = MetaDataStore.class.getDeclaredField("tables");
			field.setAccessible(true);
			@SuppressWarnings("unchecked")
			Map<String, TableKeyMetaData> map = (Map<String, TableKeyMetaData>) field.get(subject);
			assertEquals(2, map.size());
			subject.clearTable("Foo");
			assertEquals(1, map.size());
			subject.clear();
			assertEquals(0, map.size());
		}
	}

}
