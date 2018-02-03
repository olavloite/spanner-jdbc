package nl.topicus.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import nl.topicus.jdbc.metadata.AbstractCloudSpannerWrapper;

public abstract class AbstractCloudSpannerFetcher extends AbstractCloudSpannerWrapper
{
	private int fetchSize = 1;

	private int direction = ResultSet.FETCH_FORWARD;

	/**
	 * 
	 * @param direction
	 *            The fetch direction to use. Only
	 *            {@link ResultSet#FETCH_FORWARD} is supported.
	 * @throws SQLException
	 *             Thrown if any other fetch direction than
	 *             {@link ResultSet#FETCH_FORWARD} is supplied
	 */
	public void setFetchDirection(int direction) throws SQLException
	{
		if (direction != ResultSet.FETCH_FORWARD)
			throw new SQLFeatureNotSupportedException("Only FETCH_FORWARD is supported");
		this.direction = direction;
	}

	/**
	 * 
	 * @return Always returns {@link ResultSet#FETCH_FORWARD}
	 * @throws SQLException
	 *             Cannot be thrown by this method, but is added to the method
	 *             signature in order to comply with the interfaces that will be
	 *             implemented by this abstract class' concrete subclasses
	 */
	public int getFetchDirection() throws SQLException
	{
		if (this.direction != ResultSet.FETCH_FORWARD)
			throw new SQLFeatureNotSupportedException("Only FETCH_FORWARD is supported");
		return this.direction;
	}

	/**
	 * 
	 * @param rows
	 *            The number of rows to fetch
	 * @throws SQLException
	 *             Thrown if <code>rows&lt;1</code>
	 * 
	 */
	public void setFetchSize(int rows) throws SQLException
	{
		if (rows < 1)
			throw new SQLException("rows cannot be less than 1");
		this.fetchSize = rows;
	}

	/**
	 * 
	 * @return The number of rows to fetch
	 * @throws SQLException
	 *             Cannot be thrown by this method, but is added to the method
	 *             signature in order to comply with the interfaces that will be
	 *             implemented by this abstract class' concrete subclasses
	 */
	public int getFetchSize() throws SQLException
	{
		if (fetchSize < 1)
			throw new SQLException("fetchSize cannot be less than 1");
		return fetchSize;
	}

}
