package nl.topicus.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import nl.topicus.jdbc.metadata.AbstractCloudSpannerWrapper;

public abstract class AbstractCloudSpannerFetcher extends AbstractCloudSpannerWrapper
{

	public void setFetchDirection(int direction) throws SQLException
	{
		if (direction != ResultSet.FETCH_FORWARD)
			throw new SQLFeatureNotSupportedException("Only FETCH_FORWARD is supported");
	}

	public int getFetchDirection() throws SQLException
	{
		return ResultSet.FETCH_FORWARD;
	}

	public void setFetchSize(int rows) throws SQLException
	{
		// silently ignore
	}

	public int getFetchSize() throws SQLException
	{
		return 1;
	}

}
