package nl.topicus.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import nl.topicus.jdbc.test.category.UnitTest;

@Category(UnitTest.class)
public class AbstractCloudSpannerFetcherTest
{
	private AbstractCloudSpannerFetcher subject;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	public AbstractCloudSpannerFetcherTest()
	{
		subject = Mockito.spy(AbstractCloudSpannerFetcher.class);
	}

	@Test
	public void testSetFetchDirection() throws SQLException
	{
		subject.setFetchDirection(ResultSet.FETCH_FORWARD);
		thrown.expect(SQLFeatureNotSupportedException.class);
		subject.setFetchDirection(ResultSet.FETCH_REVERSE);
	}

	@Test
	public void testGetFetchDirection() throws SQLException
	{
		Assert.assertEquals(ResultSet.FETCH_FORWARD, subject.getFetchDirection());
	}

	@Test
	public void testSetFetchSize() throws SQLException
	{
		subject.setFetchSize(100);
		Assert.assertEquals(100, subject.getFetchSize());
	}

	@Test
	public void testGetFetchSize() throws SQLException
	{
		subject.setFetchSize(1);
		Assert.assertEquals(1, subject.getFetchSize());
	}

}
