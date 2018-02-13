package nl.topicus.sql2.operation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import nl.topicus.java.sql2.Operation;
import nl.topicus.java.sql2.Submission;

public abstract class CloudSpannerOperation<T> implements Operation<T>, Supplier<T>
{
	private final Executor exec;

	private Submission<T> submission;

	private Consumer<Throwable> handler;

	private long timeout;

	protected CloudSpannerOperation(Executor exec)
	{
		this.exec = exec;
	}

	private boolean isSubmitted()
	{
		return submission != null;
	}

	private void checkIsSubmitted()
	{
		if (isSubmitted())
			throw new IllegalStateException("This operation has already been submitted");
	}

	protected void handle(Throwable t)
	{
		if (handler == null)
		{
			if (t instanceof RuntimeException)
				throw (RuntimeException) t;
			throw new CompletionException(t);
		}
		else
		{
			handler.accept(t);
		}
	}

	@Override
	public Operation<T> onError(Consumer<Throwable> handler)
	{
		checkIsSubmitted();
		this.handler = handler;
		return this;
	}

	protected long timeout()
	{
		return timeout;
	}

	@Override
	public Operation<T> timeout(long milliseconds)
	{
		checkIsSubmitted();
		this.timeout = milliseconds;
		return this;
	}

	@Override
	public Submission<T> submit()
	{
		checkIsSubmitted();
		submission = new CloudSpannerSubmission<>(CompletableFuture.supplyAsync(this, exec));
		return submission;
	}

	protected Executor getExecutor()
	{
		return exec;
	}

}
