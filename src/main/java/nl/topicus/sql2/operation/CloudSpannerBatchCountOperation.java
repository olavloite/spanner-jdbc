package nl.topicus.sql2.operation;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import nl.topicus.java.sql2.BatchCountOperation;
import nl.topicus.java.sql2.ParameterizedCountOperation;
import nl.topicus.java.sql2.Result.Count;
import nl.topicus.java.sql2.Submission;
import nl.topicus.sql2.CloudSpannerConnection;

public class CloudSpannerBatchCountOperation<T> extends CloudSpannerOperation<T> implements BatchCountOperation<T>
{
	private final Executor batchExecutor = Executors.newSingleThreadExecutor();

	private Supplier<? extends T> initialValue = () -> null;
	private BiFunction<? super T, Count, ? extends T> aggregator = (in1, in2) -> null;

	private final List<CloudSpannerParameterizedCountOperation> operations = new LinkedList<>();
	private final List<Submission> submissions = new LinkedList<>();
	private final String sql;

	CloudSpannerBatchCountOperation(Executor exec, CloudSpannerConnection connection, String sql)
	{
		super(exec, connection);
		this.sql = sql;
	}

	@Override
	public T get()
	{
		T res = initialValue.get();
		for (ParameterizedCountOperation pco : operations)
		{
			res = aggregator.apply(res, u);
		}
	}

	@Override
	public Submission<T> submit()
	{
		checkIsSubmitted();
		for (CloudSpannerParameterizedCountOperation pco : operations)
		{
			submissions.add(new CloudSpannerSubmission(CompletableFuture.supplyAsync(pco, getExecutor())));
		}
		return new CloudSpannerSubmission<>(CompletableFuture.supplyAsync(this, batchExecutor));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public ParameterizedCountOperation countOperation()
	{
		CloudSpannerParameterizedCountOperation res = new CloudSpannerParameterizedCountOperation(getExecutor(),
				getConnection(), sql);
		operations.add(res);
		return res;
	}

	@Override
	public BatchCountOperation<T> initialValue(Supplier<? extends T> supplier)
	{
		this.initialValue = supplier;
		return this;
	}

	@Override
	public BatchCountOperation<T> countAggregator(BiFunction<? super T, Count, ? extends T> aggregator)
	{
		this.aggregator = aggregator;
		return this;
	}

}
