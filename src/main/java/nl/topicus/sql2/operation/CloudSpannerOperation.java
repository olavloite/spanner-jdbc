package nl.topicus.sql2.operation;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.cloud.spanner.DatabaseClient;

import nl.topicus.java.sql2.Operation;
import nl.topicus.java.sql2.Submission;
import nl.topicus.sql2.CloudSpannerConnection;

public abstract class CloudSpannerOperation<T> implements Operation<T>, Supplier<T>
{
	private final CloudSpannerConnection connection;

	private final Executor exec;

	private Submission<T> submission;

	private Consumer<Throwable> handler;

	private long timeout;

	protected CloudSpannerOperation(Executor exec, CloudSpannerConnection connection)
	{
		this.exec = exec;
		this.connection = connection;
	}

	protected CloudSpannerConnection getConnection()
	{
		return connection;
	}

	protected DatabaseClient getDbClient() throws SQLException
	{
		return connection.getDbClient();
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
