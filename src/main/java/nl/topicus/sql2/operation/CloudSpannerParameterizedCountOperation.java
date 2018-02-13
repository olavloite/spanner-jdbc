package nl.topicus.sql2.operation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import nl.topicus.java.sql2.ParameterizedCountOperation;
import nl.topicus.java.sql2.Result.Count;
import nl.topicus.java.sql2.RowOperation;
import nl.topicus.java.sql2.SqlType;

public class CloudSpannerParameterizedCountOperation<T> extends CloudSpannerParameterizedOperation<T>
		implements ParameterizedCountOperation<T>
{
	private final String sql;

	private Function<Count, T> processor;

	protected CloudSpannerParameterizedCountOperation(Executor exec, String sql)
	{
		super(exec);
		this.sql = sql;
	}

	@Override
	public T get()
	{
		return null;
	}

	@Override
	public RowOperation<T> returning(String... keys)
	{
		return null;
	}

	@Override
	public ParameterizedCountOperation<T> resultProcessor(Function<Count, T> processor)
	{
		this.processor = processor;
		return this;
	}

	@Override
	public ParameterizedCountOperation<T> set(String id, Object value)
	{
		return (ParameterizedCountOperation<T>) super.set(id, value);
	}

	@Override
	public ParameterizedCountOperation<T> set(String id, Object value, SqlType type)
	{
		return (ParameterizedCountOperation<T>) super.set(id, value, type);
	}

	@Override
	public ParameterizedCountOperation<T> set(String id, CompletableFuture<?> valueFuture)
	{
		return (ParameterizedCountOperation<T>) super.set(id, valueFuture);
	}

	@Override
	public ParameterizedCountOperation<T> set(String id, CompletableFuture<?> valueFuture, SqlType type)
	{
		return (ParameterizedCountOperation<T>) super.set(id, valueFuture, type);
	}

	@Override
	public ParameterizedCountOperation<T> timeout(long milliseconds)
	{
		return (ParameterizedCountOperation<T>) super.timeout(milliseconds);
	}

}
