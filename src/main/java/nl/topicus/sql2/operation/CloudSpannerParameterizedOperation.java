package nl.topicus.sql2.operation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import nl.topicus.java.sql2.ParameterizedOperation;
import nl.topicus.java.sql2.SqlType;
import nl.topicus.sql2.CloudSpannerConnection;

public abstract class CloudSpannerParameterizedOperation<T> extends CloudSpannerOperation<T>
		implements ParameterizedOperation<T>
{
	private ParameterStore parameterStore = new ParameterStore();

	CloudSpannerParameterizedOperation(Executor exec, CloudSpannerConnection connection)
	{
		super(exec, connection);
	}

	protected ParameterStore getParameterStore()
	{
		return parameterStore;
	}

	@Override
	public ParameterizedOperation<T> set(String id, Object value, SqlType type)
	{
		int parameterIndex = Integer.valueOf(id);
		parameterStore.setParameter(parameterIndex, value, type);
		return this;
	}

	@Override
	public ParameterizedOperation<T> set(String id, Object value)
	{
		int parameterIndex = Integer.valueOf(id);
		parameterStore.setParameter(parameterIndex, value);
		return this;
	}

	@Override
	public ParameterizedOperation<T> set(String id, CompletableFuture<?> valueFuture, SqlType type)
	{
		int parameterIndex = Integer.valueOf(id);
		parameterStore.setParameter(parameterIndex, valueFuture, type);
		return this;
	}

	@Override
	public ParameterizedOperation<T> set(String id, CompletableFuture<?> valueFuture)
	{
		int parameterIndex = Integer.valueOf(id);
		parameterStore.setParameter(parameterIndex, valueFuture);
		return this;
	}

}
