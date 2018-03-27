package nl.topicus.sql2;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.rpc.Code;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;

import nl.topicus.java.sql2.Connection;
import nl.topicus.java.sql2.ConnectionProperty;
import nl.topicus.java.sql2.Operation;
import nl.topicus.java.sql2.OperationGroup;
import nl.topicus.java.sql2.Transaction;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
import nl.topicus.sql2.connectionproperty.CloudSpannerDatabaseConnectionProperty;
import nl.topicus.sql2.connectionproperty.CloudSpannerInstanceConnectionProperty;
import nl.topicus.sql2.operation.CloudSpannerOperationGroup;

public class CloudSpannerConnection extends CloudSpannerOperationGroup<Object, Object> implements Connection
{
	public static class CloudSpannerConnectionBuilder implements Builder
	{
		private final ConnectionProperties connectionProperties;

		private Executor exec;

		CloudSpannerConnectionBuilder(ConnectionProperties connectionProperties)
		{
			this.connectionProperties = connectionProperties;
		}

		@Override
		public Builder executor(Executor exec)
		{
			if (exec == null)
				throw new NullPointerException("Executor may not be null");
			this.exec = exec;
			return this;
		}

		@Override
		public Builder property(ConnectionProperty p, Object v)
		{
			return this;
		}

		@Override
		public CloudSpannerConnection build()
		{
			if (exec == null)
				exec = createDefaultExecutor();
			return new CloudSpannerConnection(connectionProperties, exec);
		}

		private Executor createDefaultExecutor()
		{
			/**
			 * Default to an Executor that runs 1 task at a time
			 * (corePoolSize=1), but can queue an unlimited number of tasks.
			 */
			return Executors.newSingleThreadScheduledExecutor();
		}

	}

	private static final class WaitForConnectOperationListener implements ConnectionLifecycleListener
	{
		private final CountDownLatch latch = new CountDownLatch(1);

		private static void waitForConnection(Connection connection, long timeout, TimeUnit unit) throws SQLException
		{
			WaitForConnectOperationListener listener = new WaitForConnectOperationListener();
			connection.registerLifecycleListener(listener);
			boolean res = false;
			if (!(connection.getLifecycle() == Lifecycle.OPEN || connection.getLifecycle() == Lifecycle.CLOSED))
			{
				try
				{
					res = listener.latch.await(timeout, unit);
				}
				catch (InterruptedException e)
				{
					throw new CloudSpannerSQLException("Wait interrupted", Code.UNKNOWN, e);
				}
			}
			if (!(connection.getLifecycle() == Lifecycle.OPEN || connection.getLifecycle() == Lifecycle.CLOSED))
			{
				if (!res)
				{
					throw new CloudSpannerSQLException("Timeout", Code.DEADLINE_EXCEEDED);
				}
				else
				{
					throw new CloudSpannerSQLException("Failed to open connection", Code.UNKNOWN);
				}
			}
		}

		private WaitForConnectOperationListener()
		{
		}

		@Override
		public void lifecycleEvent(Connection conn, Lifecycle previous, Lifecycle current)
		{
			if (current == Lifecycle.CLOSED || current == Lifecycle.OPEN)
			{
				latch.countDown();
			}
		}

	}

	private final ConnectionProperties connectionProperties;

	private Lifecycle lifecycle = Lifecycle.NEW;

	private List<ConnectionLifecycleListener> lifecycleListeners = new ArrayList<>();

	private Spanner spanner;

	private String clientId;

	private DatabaseClient dbClient;

	private DatabaseAdminClient adminClient;

	private CloudSpannerConnection(ConnectionProperties connectionProperties, Executor exec)
	{
		super(exec);
		this.connectionProperties = connectionProperties;
	}

	@Override
	protected CloudSpannerConnection getConnection()
	{
		return this;
	}

	@Override
	public Operation<Void> connectOperation()
	{
		return new CloudSpannerConnectOperation(getExecutor(), this, connectionProperties);
	}

	Spanner getSpanner() throws SQLException
	{
		waitForConnect();
		return spanner;
	}

	void doConnect(Spanner spanner, String clientId, DatabaseClient dbClient, DatabaseAdminClient adminClient)
			throws SQLException
	{
		this.spanner = spanner;
		this.clientId = clientId;
		this.dbClient = dbClient;
		this.adminClient = adminClient;
		if (lifecycle == Lifecycle.NEW)
			setLifecycle(Lifecycle.OPEN);
		else if (lifecycle == Lifecycle.NEW_INACTIVE)
			setLifecycle(Lifecycle.INACTIVE);
		else
		{
			throw new CloudSpannerSQLException("Invalid lifecycle for finalizing connect: " + lifecycle,
					Code.FAILED_PRECONDITION);
		}
	}

	void setLifecycle(Lifecycle lc)
	{
		Lifecycle prev = this.lifecycle;
		this.lifecycle = lc;
		lifecycleListeners.stream().forEach(listener -> listener.lifecycleEvent(this, prev, lc));
	}

	/**
	 * Convenience method for waiting for a connect operation to finish. When
	 * using the {@link DataSource#getConnection()}, the method may return
	 * before the connection is actually ready for use. Calling this method will
	 * block until the connection is ready.
	 * 
	 * @throws SQLException
	 */
	public void waitForConnect() throws SQLException
	{
		waitForConnect(DEFAULT_WAIT_FOR_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
	}

	/**
	 * Convenience method for waiting for a connect operation to finish. When
	 * using the {@link DataSource#getConnection()}, the method may return
	 * before the connection is actually ready for use. Calling this method will
	 * block until the connection is ready.
	 * 
	 * @param timeout
	 * @param unit
	 * @throws SQLException
	 */
	public void waitForConnect(long timeout, TimeUnit unit) throws SQLException
	{
		WaitForConnectOperationListener.waitForConnection(this, timeout, unit);
	}

	public boolean getAutoCommit()
	{
		return true;
	}

	private static final long DEFAULT_WAIT_FOR_CONNECT_TIMEOUT = 10000l;

	@Override
	public DatabaseClient getDbClient() throws SQLException
	{
		waitForConnect(DEFAULT_WAIT_FOR_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
		return dbClient;
	}

	@Override
	public Operation<Void> validationOperation(Validation depth)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operation<Void> closeOperation()
	{
		return new CloudSpannerCloseOperation(getExecutor(), this);
	}

	void doClose() throws SQLException
	{
		setLifecycle(Lifecycle.CLOSED);
	}

	@Override
	public <S, T> OperationGroup<S, T> operationGroup()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Transaction getTransaction()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void registerLifecycleListener(ConnectionLifecycleListener listener)
	{
		lifecycleListeners.add(listener);
	}

	public void removeLifecycleListener(ConnectionLifecycleListener listener)
	{
		lifecycleListeners.remove(listener);
	}

	@Override
	public Connection abort()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Lifecycle getLifecycle()
	{
		return lifecycle;
	}

	@Override
	public Map<ConnectionProperty, Object> getProperties()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void onSubscribe(Subscription subscription)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public Connection activate()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Connection deactivate()
	{
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Execute one or more DDL-statements on the database and wait for it to
	 * finish or return after syntax check (when running in async mode). Calling
	 * this method will also automatically commit the currently running
	 * transaction.
	 * 
	 * @param inputSql
	 *            The DDL-statement(s) to execute. Some statements may end up
	 *            not being sent to Cloud Spanner if they contain IF [NOT]
	 *            EXISTS clauses. The driver will check whether the condition is
	 *            met, and only then will it be sent to Cloud Spanner.
	 * @return The number of statements executed
	 * @throws SQLException
	 *             If an error occurs during the execution of the statement.
	 */
	public Long executeDDL(List<String> inputSql) throws SQLException
	{
		if (!getAutoCommit())
			commit();
		// Check for IF [NOT] EXISTS statements
		// TODO: Implement
		// List<String> sql = DDLStatement.getActualSql(this, inputSql);
		List<String> sql = inputSql;
		if (!sql.isEmpty())
		{
			try
			{
				com.google.cloud.spanner.Operation<Void, UpdateDatabaseDdlMetadata> operation = adminClient
						.updateDatabaseDdl(
								(String) connectionProperties.get(CloudSpannerInstanceConnectionProperty.class),
								(String) connectionProperties.get(CloudSpannerDatabaseConnectionProperty.class), sql,
								null);
				do
				{
					operation = operation.waitFor();
				}
				while (!operation.isDone());
				return Long.valueOf(sql.size());
			}
			catch (SpannerException e)
			{
				throw new CloudSpannerSQLException(
						"Could not execute DDL statement(s) " + String.join("\n;\n", sql) + ": " + e.getMessage(), e);
			}
		}
		return 0L;
	}

}
