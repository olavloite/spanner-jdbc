package nl.topicus.sql2;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Subscription;

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
		public Connection build()
		{
			if (exec == null)
				exec = createDefaultExecutor();
			return new CloudSpannerConnection(connectionProperties, exec);
		}

		private Executor createDefaultExecutor()
		{
			return Executors.newFixedThreadPool(10);
		}

	}

	private final ConnectionProperties connectionProperties;

	private Lifecycle lifecycle = Lifecycle.NEW;

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

	void doConnect(Spanner spanner, String clientId, DatabaseClient dbClient, DatabaseAdminClient adminClient)
			throws SQLException
	{
		this.spanner = spanner;
		this.clientId = clientId;
		this.dbClient = dbClient;
		this.adminClient = adminClient;
		if (lifecycle == Lifecycle.NEW)
			lifecycle = Lifecycle.OPEN;
		else if (lifecycle == Lifecycle.NEW_INACTIVE)
			lifecycle = Lifecycle.INACTIVE;
		else
			throw new CloudSpannerSQLException("Invalid lifecycle for finalizing connect: " + lifecycle,
					Code.FAILED_PRECONDITION);
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
		// TODO Auto-generated method stub

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
