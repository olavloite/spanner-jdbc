package nl.topicus.jdbc;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class CloudSpannerDatabaseMetaDataConstantsTest
{
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testInstantiation() throws Exception
	{
		thrown.expect(IllegalAccessException.class);
		CloudSpannerDatabaseMetaDataConstants.class.newInstance();
	}

	@Test
	public void testConstructor()
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		Constructor<?> constructor = CloudSpannerDatabaseMetaDataConstants.class.getDeclaredConstructors()[0];
		Assert.assertTrue(Modifier.isPrivate(constructor.getModifiers()));
		constructor.setAccessible(true);
		thrown.expect(InstantiationException.class);
		constructor.newInstance();
	}

}
