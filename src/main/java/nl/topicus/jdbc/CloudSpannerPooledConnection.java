package nl.topicus.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

import com.google.rpc.Code;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;

/**
 * Implementation of the PooledConnection interface. This implementation is
 * based on the implementation of PostgreSQL.
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
public class CloudSpannerPooledConnection implements PooledConnection, AutoCloseable
{
	private final List<ConnectionEventListener> listeners = new LinkedList<>();
	private Connection con;
	private ConnectionHandler last;
	private final boolean autoCommit;
	private final boolean isXA;

	/**
	 * Creates a new PooledConnection representing the specified physical
	 * connection.
	 *
	 * @param con
	 *            connection
	 * @param autoCommit
	 *            whether to autocommit
	 * @param isXA
	 *            whether connection is a XA connection
	 */
	public CloudSpannerPooledConnection(Connection con, boolean autoCommit, boolean isXA)
	{
		this.con = con;
		this.autoCommit = autoCommit;
		this.isXA = isXA;
	}

	public CloudSpannerPooledConnection(Connection con, boolean autoCommit)
	{
		this(con, autoCommit, false);
	}

	/**
	 * Adds a listener for close or fatal error events on the connection handed
	 * out to a client.
	 */
	@Override
	public void addConnectionEventListener(ConnectionEventListener connectionEventListener)
	{
		listeners.add(connectionEventListener);
	}

	/**
	 * Removes a listener for close or fatal error events on the connection
	 * handed out to a client.
	 */
	@Override
	public void removeConnectionEventListener(ConnectionEventListener connectionEventListener)
	{
		listeners.remove(connectionEventListener);
	}

	/**
	 * Closes the physical database connection represented by this
	 * PooledConnection. If any client has a connection based on this
	 * PooledConnection, it is forcibly closed as well.
	 */
	@Override
	public void close() throws SQLException
	{
		if (last != null)
		{
			last.close();
			if (!con.isClosed() && !con.getAutoCommit())
			{
				try
				{
					con.rollback();
				}
				catch (SQLException ignored)
				{
					// ignore
				}
			}
		}
		try
		{
			con.close();
		}
		finally
		{
			con = null;
		}
	}

	/**
	 * Gets a handle for a client to use. This is a wrapper around the physical
	 * connection, so the client can call close and it will just return the
	 * connection to the pool without really closing the physical connection.
	 *
	 * <p>
	 * According to the JDBC 2.0 Optional Package spec (6.2.3), only one client
	 * may have an active handle to the connection at a time, so if there is a
	 * previous handle active when this is called, the previous one is forcibly
	 * closed and its work rolled back.
	 * </p>
	 */
	@Override
	public ICloudSpannerConnection getConnection() throws SQLException
	{
		if (con == null)
		{
			// Before throwing the exception, let's notify the registered
			// listeners about the error
			SQLException sqlException = new CloudSpannerSQLException("This PooledConnection has already been closed.",
					Code.FAILED_PRECONDITION);
			fireConnectionFatalError(sqlException);
			throw sqlException;
		}
		// If any error occurs while opening a new connection, the listeners
		// have to be notified. This gives a chance to connection pools to
		// eliminate bad pooled connections.
		try
		{
			// Only one connection can be open at a time from this
			// PooledConnection. See JDBC 2.0 Optional
			// Package spec section 6.2.3
			if (last != null)
			{
				last.close();
				if (!con.getAutoCommit())
				{
					rollbackAndIgnoreException();
				}
				con.clearWarnings();
			}
			/*
			 * In XA-mode, autocommit is handled in PGXAConnection, because it
			 * depends on whether an XA-transaction is open or not
			 */
			if (!isXA)
			{
				con.setAutoCommit(autoCommit);
			}
		}
		catch (SQLException sqlException)
		{
			fireConnectionFatalError(sqlException);
			throw (SQLException) sqlException.fillInStackTrace();
		}
		ConnectionHandler handler = new ConnectionHandler(con);
		last = handler;

		ICloudSpannerConnection proxyCon = (ICloudSpannerConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
				new Class[] { Connection.class, ICloudSpannerConnection.class }, handler);
		last.setProxy(proxyCon);
		return proxyCon;
	}

	private void rollbackAndIgnoreException()
	{
		try
		{
			con.rollback();
		}
		catch (SQLException ignored)
		{
			// ignore exception
		}
	}

	/**
	 * Used to fire a connection closed event to all listeners.
	 */
	void fireConnectionClosed()
	{
		ConnectionEvent evt = null;
		// Copy the listener list so the listener can remove itself during this
		// method call
		ConnectionEventListener[] local = listeners.toArray(new ConnectionEventListener[listeners.size()]);
		for (ConnectionEventListener listener : local)
		{
			if (evt == null)
			{
				evt = createConnectionEvent(null);
			}
			listener.connectionClosed(evt);
		}
	}

	/**
	 * Used to fire a connection error event to all listeners.
	 */
	void fireConnectionFatalError(SQLException e)
	{
		ConnectionEvent evt = null;
		// Copy the listener list so the listener can remove itself during this
		// method call
		ConnectionEventListener[] local = listeners.toArray(new ConnectionEventListener[listeners.size()]);
		for (ConnectionEventListener listener : local)
		{
			if (evt == null)
			{
				evt = createConnectionEvent(e);
			}
			listener.connectionErrorOccurred(evt);
		}
	}

	protected ConnectionEvent createConnectionEvent(SQLException e)
	{
		return new ConnectionEvent(this, e);
	}

	private static final Set<Code> FATAL_CODES = new HashSet<>();
	static
	{
		FATAL_CODES.add(Code.UNAUTHENTICATED);
		FATAL_CODES.add(Code.DATA_LOSS);
		FATAL_CODES.add(Code.INTERNAL);
	}

	private static boolean isFatalState(Code code)
	{
		if (code == null)
		{
			// no info, assume fatal
			return true;
		}
		return FATAL_CODES.contains(code);
	}

	/**
	 * Fires a connection error event, but only if we think the exception is
	 * fatal.
	 *
	 * @param e
	 *            the SQLException to consider
	 */
	private void fireConnectionError(SQLException e)
	{
		Code code = Code.UNKNOWN;
		if (e instanceof CloudSpannerSQLException)
		{
			code = ((CloudSpannerSQLException) e).getCode();
		}
		if (!isFatalState(code))
		{
			return;
		}
		fireConnectionFatalError(e);
	}

	/**
	 * Instead of declaring a class implementing Connection, which would have to
	 * be updated for every JDK rev, use a dynamic proxy to handle all calls
	 * through the Connection interface. This is the part that requires JDK 1.3
	 * or higher, though JDK 1.2 could be supported with a 3rd-party proxy
	 * package.
	 */
	private class ConnectionHandler implements InvocationHandler
	{
		private Connection con;
		private Connection proxy; // the Connection the client is currently
									// using, which is a proxy
		private boolean automatic = false;

		public ConnectionHandler(Connection con)
		{
			this.con = con;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
		{
			final String methodName = method.getName();
			// From Object
			if (method.getDeclaringClass().equals(Object.class))
			{
				return handleInvokeObjectMethod(method, args);
			}

			// All the rest is from the Connection or PGConnection interface
			if (methodName.equals("isClosed"))
			{
				return con == null || con.isClosed();
			}
			if (methodName.equals("close"))
			{
				return handleInvokeClose();
			}
			if (con == null || con.isClosed())
			{
				throw new CloudSpannerSQLException(automatic
						? "Connection has been closed automatically because a new connection was opened for the same PooledConnection or the PooledConnection has been closed."
						: "Connection has been closed.", Code.FAILED_PRECONDITION);
			}

			// From here on in, we invoke via reflection, catch exceptions,
			// and check if they're fatal before rethrowing.
			try
			{
				if (methodName.equals("createStatement"))
				{
					Statement st = (Statement) method.invoke(con, args);
					return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] { Statement.class },
							new StatementHandler(this, st));
				}
				else if (methodName.equals("prepareCall"))
				{
					Statement st = (Statement) method.invoke(con, args);
					return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] { CallableStatement.class },
							new StatementHandler(this, st));
				}
				else if (methodName.equals("prepareStatement"))
				{
					Statement st = (Statement) method.invoke(con, args);
					return Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] { PreparedStatement.class },
							new StatementHandler(this, st));
				}
				else
				{
					return method.invoke(con, args);
				}
			}
			catch (final InvocationTargetException ite)
			{
				final Throwable te = ite.getTargetException();
				if (te instanceof SQLException)
				{
					fireConnectionError((SQLException) te); // Tell listeners
															// about exception
															// if it's fatal
				}
				throw te;
			}
		}

		private Object handleInvokeObjectMethod(Method method, Object[] args) throws Throwable
		{
			final String methodName = method.getName();
			if (methodName.equals("toString"))
			{
				return "Pooled connection wrapping physical connection " + con;
			}
			if (methodName.equals("equals"))
			{
				return proxy == args[0];
			}
			if (methodName.equals("hashCode"))
			{
				return System.identityHashCode(proxy);
			}
			try
			{
				return method.invoke(con, args);
			}
			catch (InvocationTargetException e)
			{
				throw e.getTargetException();
			}
		}

		private Object handleInvokeClose() throws SQLException
		{
			// we are already closed and a double close
			// is not an error.
			if (con == null)
			{
				return null;
			}

			SQLException ex = null;
			if (!con.isClosed())
			{
				if (!isXA && !con.getAutoCommit())
				{
					try
					{
						con.rollback();
					}
					catch (SQLException e)
					{
						ex = e;
					}
				}
				con.clearWarnings();
			}
			con = null;
			this.proxy = null;
			last = null;
			fireConnectionClosed();
			if (ex != null)
			{
				throw ex;
			}
			return null;
		}

		Connection getProxy()
		{
			return proxy;
		}

		void setProxy(Connection proxy)
		{
			this.proxy = proxy;
		}

		public void close()
		{
			if (con != null)
			{
				automatic = true;
			}
			con = null;
			proxy = null;
			// No close event fired here: see JDBC 2.0 Optional Package spec
			// section 6.3
		}

		@SuppressWarnings("unused")
		public boolean isClosed()
		{
			return con == null;
		}
	}

	/**
	 * Instead of declaring classes implementing Statement, PreparedStatement,
	 * and CallableStatement, which would have to be updated for every JDK rev,
	 * use a dynamic proxy to handle all calls through the Statement interfaces.
	 * This is the part that requires JDK 1.3 or higher, though JDK 1.2 could be
	 * supported with a 3rd-party proxy package.
	 *
	 * The StatementHandler is required in order to return the proper Connection
	 * proxy for the getConnection method.
	 */
	private class StatementHandler implements InvocationHandler
	{
		private ConnectionHandler con;
		private Statement st;

		public StatementHandler(ConnectionHandler con, Statement st)
		{
			this.con = con;
			this.st = st;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
		{
			final String methodName = method.getName();
			// From Object
			if (method.getDeclaringClass().equals(Object.class))
			{
				return handleInvokeObjectMethod(proxy, method, args);
			}

			// All the rest is from the Statement interface
			if (methodName.equals("isClosed"))
			{
				return st == null || st.isClosed();
			}
			if (methodName.equals("close"))
			{
				return handleInvokeClose();
			}
			if (st == null || st.isClosed())
			{
				throw new CloudSpannerSQLException("Statement has been closed.", Code.FAILED_PRECONDITION);
			}
			if (methodName.equals("getConnection"))
			{
				return con.getProxy(); // the proxied connection, not a physical
										// connection
			}

			// Delegate the call to the proxied Statement.
			try
			{
				return method.invoke(st, args);
			}
			catch (final InvocationTargetException ite)
			{
				final Throwable te = ite.getTargetException();
				if (te instanceof SQLException)
				{
					fireConnectionError((SQLException) te); // Tell listeners
															// about exception
															// if it's fatal
				}
				throw te;
			}
		}

		private Object handleInvokeObjectMethod(Object proxy, Method method, Object[] args)
				throws IllegalAccessException, IllegalArgumentException, InvocationTargetException
		{
			final String methodName = method.getName();
			if (methodName.equals("toString"))
			{
				return "Pooled statement wrapping physical statement " + st;
			}
			if (methodName.equals("hashCode"))
			{
				return System.identityHashCode(proxy);
			}
			if (methodName.equals("equals"))
			{
				return proxy == args[0];
			}
			return method.invoke(st, args);
		}

		private Object handleInvokeClose() throws SQLException
		{
			if (st == null || st.isClosed())
			{
				return null;
			}
			con = null;
			final Statement oldSt = st;
			st = null;
			oldSt.close();
			return null;
		}
	}

	/**
	 * This implementation does nothing as the driver does not support pooled
	 * statements.
	 * 
	 * @see PooledConnection#removeStatementEventListener(StatementEventListener)
	 */
	@Override
	public void removeStatementEventListener(StatementEventListener listener)
	{
		// do nothing as pooled statements are not supported
	}

	/**
	 * This implementation does nothing as the driver does not support pooled
	 * statements.
	 * 
	 * @see PooledConnection#removeStatementEventListener(StatementEventListener)
	 */
	@Override
	public void addStatementEventListener(StatementEventListener listener)
	{
		// do nothing as pooled statements are not supported
	}

	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException("Method getParentLogger() is not supported");
	}

}
