package nl.topicus.jdbc;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import javax.sql.ConnectionPoolDataSource;

/**
 * Implementation of the ConnectionPoolDataSource interface. This implementation
 * is based on the implementation of PostgreSQL.
 * 
 * PostgreSQL is released under the PostgreSQL License, a liberal Open Source
 * license, similar to the BSD or MIT licenses.
 *
 * PostgreSQL Database Management System (formerly known as Postgres, then as
 * Postgres95)
 *
 * Portions Copyright (c) 1996-2017, The PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 1994, The Regents of the University of California
 *
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
 * "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
 * MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 * 
 * @author loite
 */
public class CloudSpannerConnectionPoolDataSource extends CloudSpannerDataSource
		implements ConnectionPoolDataSource, Serializable
{
	private static final long serialVersionUID = 1L;
	private boolean defaultAutoCommit = true;

	/**
	 * 
	 * @return A description of this data source
	 */
	public String getDescription()
	{
		return "ConnectionPoolDataSource from " + nl.topicus.jdbc.CloudSpannerDriver.getVersion();
	}

	/**
	 * Gets a connection which may be pooled by the app server or middleware
	 * implementation of DataSource.
	 *
	 * @throws java.sql.SQLException
	 *             Occurs when the physical database connection cannot be
	 *             established.
	 */
	@Override
	public CloudSpannerPooledConnection getPooledConnection() throws SQLException
	{
		return new CloudSpannerPooledConnection(getConnection(), defaultAutoCommit);
	}

	/**
	 * Gets a connection which may be pooled by the app server or middleware
	 * implementation of DataSource.
	 *
	 * @throws java.sql.SQLException
	 *             Occurs when the physical database connection cannot be
	 *             established.
	 */
	@Override
	public CloudSpannerPooledConnection getPooledConnection(String user, String password) throws SQLException
	{
		return new CloudSpannerPooledConnection(getConnection(user, password), defaultAutoCommit);
	}

	/**
	 * Gets whether connections supplied by this pool will have autoCommit
	 * turned on by default. The default value is <tt>false</tt>, so that
	 * autoCommit will be turned off by default.
	 *
	 * @return true if connections supplied by this pool will have autoCommit
	 */
	public boolean isDefaultAutoCommit()
	{
		return defaultAutoCommit;
	}

	/**
	 * Sets whether connections supplied by this pool will have autoCommit
	 * turned on by default. The default value is <tt>false</tt>, so that
	 * autoCommit will be turned off by default.
	 *
	 * @param defaultAutoCommit
	 *            whether connections supplied by this pool will have autoCommit
	 */
	public void setDefaultAutoCommit(boolean defaultAutoCommit)
	{
		this.defaultAutoCommit = defaultAutoCommit;
	}

	@Override
	public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException("Method getParentLogger() is not supported");
	}

}
