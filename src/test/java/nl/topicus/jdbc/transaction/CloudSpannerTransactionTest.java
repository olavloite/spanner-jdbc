package nl.topicus.jdbc.transaction;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.test.category.UnitTest;
import nl.topicus.jdbc.test.util.CloudSpannerTestObjects;

@Category(UnitTest.class)
public class CloudSpannerTransactionTest
{

	/**
	 * Run some simple tests for methods that are needed for the interface, but
	 * do not do anything
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testInterfaceMethods() throws SQLException
	{
		CloudSpannerConnection connection = CloudSpannerTestObjects.createConnection();
		try (CloudSpannerTransaction tx = new CloudSpannerTransaction(null, connection))
		{
			Assert.assertNull(tx.read(null, null, null));
			Assert.assertNull(tx.readUsingIndex(null, null, null, null));
			Assert.assertNull(tx.readRow(null, null, null));
			Assert.assertNull(tx.readRowUsingIndex(null, null, null, null));
			Assert.assertNull(tx.analyzeQuery(null, null));
		}
	}

}
