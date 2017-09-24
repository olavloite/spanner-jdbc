package nl.topicus.jdbc;

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

}
