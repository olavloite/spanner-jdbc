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

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Spanner;
import com.google.rpc.Code;

import nl.topicus.java.sql2.Connection;
import nl.topicus.java.sql2.ConnectionProperty;
import nl.topicus.java.sql2.Operation;
import nl.topicus.java.sql2.OperationGroup;
import nl.topicus.java.sql2.Transaction;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;
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
			// Default 1 thread as execution directly on the connection should
			// be sequential
			return Executors.newFixedThreadPool(1);
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

	Spanner getSpanner()
	{
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

	public void waitForConnect(long timeout, TimeUnit unit) throws SQLException
	{
		WaitForConnectOperationListener.waitForConnection(this, timeout, unit);
	}

	public boolean getAutoCommit()
	{
		return true;
	}

	@Override
	public DatabaseClient getDbClient()
	{
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

}
