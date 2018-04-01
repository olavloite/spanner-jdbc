package nl.topicus.jdbc;

import org.junit.Before;
import org.junit.experimental.categories.Category;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class BatchReadOnlyTest
{
	private CloudSpannerConnection connection;

	@Before
	public void setup()
	{
		connection = new CloudSpannerConnection();
	}

}
