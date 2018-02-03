package nl.topicus.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import javax.sql.XADataSource;

import nl.topicus.jdbc.xa.CloudSpannerXAConnection;

/**
 * Implementation of the XADataSource interface. This implementation is based on
 * the implementation of PostgreSQL.
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
 * MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS. XA-enabled
 * DataSource implementation.
 *
 * @author Heikki Linnakangas (heikki.linnakangas@iki.fi)
 * @author loite
 */
public class CloudSpannerXADataSource extends CloudSpannerDataSource implements XADataSource
{
	private boolean createXATable = true;

	@Override
	public CloudSpannerXAConnection getXAConnection() throws SQLException
	{
		Connection con = super.getConnection();
		return new CloudSpannerXAConnection((CloudSpannerConnection) con, isCreateXATable());
	}

	@Override
	public CloudSpannerXAConnection getXAConnection(String user, String password) throws SQLException
	{
		return getXAConnection();
	}

	public String getDescription()
	{
		return "JDBC3 XA-enabled DataSource from " + CloudSpannerDriver.getVersion();
	}

	@Override
	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException("Method getParentLogger() is not supported");
	}

	public boolean isCreateXATable()
	{
		return createXATable;
	}

	public void setCreateXATable(boolean createXATable)
	{
		this.createXATable = createXATable;
	}
}
