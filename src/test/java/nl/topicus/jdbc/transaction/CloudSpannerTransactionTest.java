package nl.topicus.jdbc.transaction;

import java.sql.SQLException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.cloud.spanner.SpannerException;

import nl.topicus.jdbc.CloudSpannerConnection;
import nl.topicus.jdbc.test.category.UnitTest;
import nl.topicus.jdbc.test.util.CloudSpannerTestObjects;

@Category(UnitTest.class)
public class CloudSpannerTransactionTest
{

	@Test(expected = SpannerException.class)
	public void testRead() throws SQLException
	{
		CloudSpannerConnection connection = CloudSpannerTestObjects.createConnection();
		try (CloudSpannerTransaction tx = new CloudSpannerTransaction(null, null, connection))
		{
			tx.read(null, null, null);
		}
	}

	@Test(expected = SpannerException.class)
	public void testReadUsingIndex() throws SQLException
	{
		CloudSpannerConnection connection = CloudSpannerTestObjects.createConnection();
		try (CloudSpannerTransaction tx = new CloudSpannerTransaction(null, null, connection))
		{
			tx.readUsingIndex(null, null, null, null);
		}
	}

	@Test(expected = SpannerException.class)
	public void testReadRow() throws SQLException
	{
		CloudSpannerConnection connection = CloudSpannerTestObjects.createConnection();
		try (CloudSpannerTransaction tx = new CloudSpannerTransaction(null, null, connection))
		{
			tx.readRow(null, null, null);
		}
	}

	@Test(expected = SpannerException.class)
	public void testReadRowUsingIndex() throws SQLException
	{
		CloudSpannerConnection connection = CloudSpannerTestObjects.createConnection();
		try (CloudSpannerTransaction tx = new CloudSpannerTransaction(null, null, connection))
		{
			tx.readRowUsingIndex(null, null, null, null);
		}
	}

	@Test(expected = SpannerException.class)
	public void testAnalyzeQuery() throws SQLException
	{
		CloudSpannerConnection connection = CloudSpannerTestObjects.createConnection();
		try (CloudSpannerTransaction tx = new CloudSpannerTransaction(null, null, connection))
		{
			tx.analyzeQuery(null, null);
		}
	}

}
