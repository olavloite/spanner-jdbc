package nl.topicus.sql2.operation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow.Publisher;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.logging.Logger;

import nl.topicus.java.sql2.BatchCountOperation;
import nl.topicus.java.sql2.DynamicMultiOperation;
import nl.topicus.java.sql2.LocalOperation;
import nl.topicus.java.sql2.Operation;
import nl.topicus.java.sql2.OperationGroup;
import nl.topicus.java.sql2.OutOperation;
import nl.topicus.java.sql2.ParameterizedCountOperation;
import nl.topicus.java.sql2.ParameterizedRowOperation;
import nl.topicus.java.sql2.PublisherOperation;
import nl.topicus.java.sql2.StaticMultiOperation;
import nl.topicus.java.sql2.TransactionOutcome;
import nl.topicus.sql2.CloudSpannerConnection;

public class CloudSpannerOperationGroup<S, T> extends CloudSpannerOperation<T> implements OperationGroup<S, T>
{
	private boolean held = false;

	protected CloudSpannerOperationGroup(Executor exec)
	{
		this(exec, null);
	}

	CloudSpannerOperationGroup(Executor exec, CloudSpannerConnection connection)
	{
		super(exec, connection);
	}

	@Override
	public OperationGroup<S, T> parallel()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationGroup<S, T> independent()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationGroup<S, T> conditional(CompletableFuture<Boolean> condition)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationGroup<S, T> holdForMoreMembers()
	{
		this.held = true;
		return this;
	}

	@Override
	public OperationGroup<S, T> releaseProhibitingMoreMembers()
	{
		this.held = false;
		return this;
	}

	@Override
	public OperationGroup<S, T> initialValue(Supplier<T> supplier)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationGroup<S, T> memberAggregator(BiFunction<T, S, T> aggregator)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R extends S> BatchCountOperation<R> batchCountOperation(String sql)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R extends S> ParameterizedCountOperation<R> countOperation(String sql)
	{
		return new CloudSpannerParameterizedCountOperation<>(getExecutor(), getConnection(), sql);
	}

	@Override
	public Operation<Void> operation(String sql)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R extends S> OutOperation<R> outOperation(String sql)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R extends S> ParameterizedRowOperation<R> rowOperation(String sql)
	{
		return new CloudSpannerParameterizedRowOperation<>(getExecutor(), getConnection(), sql);
	}

	@Override
	public <R extends S> StaticMultiOperation<R> staticMultiOperation(String sql)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R extends S> PublisherOperation<R> publisherOperation(String sql)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R extends S> DynamicMultiOperation<R> dynamicMultiOperation(String sql)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operation<TransactionOutcome> commitOperation()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Operation<TransactionOutcome> rollbackOperation()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LocalOperation<T> localOperation()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public OperationGroup<S, T> operationPublisher(Publisher<Operation> publisher)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationGroup<S, T> logger(Logger logger)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationGroup<S, T> timeout(long milliseconds)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T get()
	{
		// TODO Auto-generated method stub
		return null;
	}

}
