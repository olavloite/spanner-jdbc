package nl.topicus.jdbc.statement;

import java.sql.SQLFeatureNotSupportedException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class ConversionResultTest
{

	@Test
	public void testToString()
	{
		ConversionResult res = new ConversionResult(1000l, 10000l, System.currentTimeMillis(),
				System.currentTimeMillis() + 1000l, new SQLFeatureNotSupportedException());
		String str = res.toString();
		Assert.assertNotNull(str);
		Assert.assertTrue(str.contains("Exception"));
	}

}
