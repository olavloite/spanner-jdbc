package nl.topicus.jdbc.transaction;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.rpc.Code;

import nl.topicus.jdbc.exception.CloudSpannerSQLException;

class TransactionThread extends Thread
{
	public static class QueryException extends RuntimeException
	{
		private static final long serialVersionUID = 1L;

		private QueryException(String message, Throwable cause)
		{
			super(message, cause);
		}
	}

	private enum TransactionStatus
	{
		NOT_STARTED, RUNNING, SUCCESS, FAIL;
	}

	private final Object monitor = new Object();

	private DatabaseClient dbClient;

	private boolean stop;

	private boolean stopped;

	private TransactionStatus status = TransactionStatus.NOT_STARTED;

	private Timestamp commitTimestamp;

	private Exception exception;

	private boolean commit;

	private List<Mutation> mutations = new ArrayList<>(40);

	private BlockingQueue<Statement> statements = new LinkedBlockingQueue<>();

	private BlockingQueue<ResultSet> resultSets = new LinkedBlockingQueue<>();

	TransactionThread(DatabaseClient dbClient)
	{
		this.dbClient = dbClient;
		setDaemon(true);
	}

	@Override
	public void run()
	{
		status = TransactionStatus.RUNNING;
		TransactionRunner runner = dbClient.readWriteTransaction();
		synchronized (monitor)
		{
			try
			{
				status = runner.run(new TransactionCallable<TransactionStatus>()
				{

					@Override
					public TransactionStatus run(TransactionContext transaction) throws Exception
					{
						while (!stop)
						{
							try
							{
								Statement statement = statements.poll(5, TimeUnit.SECONDS);
								if (statement != null)
								{
									if (!(statement.getSql().equals("commit") || statement.getSql().equals("rollback")))
									{
										resultSets.put(transaction.executeQuery(statement));
									}
								}
								else
								{
									// keep alive
									try (ResultSet rs = transaction.executeQuery(Statement.of("SELECT 1")))
									{
										rs.next();
									}
								}
							}
							catch (InterruptedException e)
							{
								stopped = true;
								exception = e;
								throw e;
							}
						}

						if (commit)
						{
							transaction.buffer(mutations);
						}
						stopped = true;
						return TransactionStatus.SUCCESS;
					}
				});
				commitTimestamp = runner.getCommitTimestamp();
			}
			catch (Exception e)
			{
				status = TransactionStatus.FAIL;
				exception = e;
			}
			finally
			{
				monitor.notifyAll();
			}
		}
	}

	ResultSet executeQuery(Statement statement)
	{
		try
		{
			statements.put(statement);
			return resultSets.take();
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			throw new QueryException("Query execution interrupted", e);
		}
	}

	void buffer(Mutation mutation)
	{
		if (mutation == null)
			throw new NullPointerException("Mutation is null");
		mutations.add(mutation);
	}

	void buffer(Iterable<Mutation> mutations)
	{
		Iterator<Mutation> it = mutations.iterator();
		while (it.hasNext())
			buffer(it.next());
	}

	Timestamp commit() throws SQLException
	{
		stopTransaction(true);
		return commitTimestamp;
	}

	void rollback() throws SQLException
	{
		stopTransaction(false);
	}

	private void stopTransaction(boolean commit) throws SQLException
	{
		if (status == TransactionStatus.FAIL || status == TransactionStatus.SUCCESS)
			return;

		this.commit = commit;
		stop = true;
		// Add a null object in order to get the transaction thread to proceed
		statements.add(Statement.of(commit ? "commit" : "rollback"));
		synchronized (monitor)
		{
			while (!stopped || status == TransactionStatus.NOT_STARTED || status == TransactionStatus.RUNNING)
			{
				try
				{
					monitor.wait();
				}
				catch (InterruptedException e)
				{
					Thread.currentThread().interrupt();
					throw new CloudSpannerSQLException(
							(commit ? "Commit failed: " : "Rollback failed: ") + e.getMessage(), Code.ABORTED, e);
				}
			}
		}
		if (status == TransactionStatus.FAIL && exception != null)
		{
			Code code = Code.UNKNOWN;
			if (exception instanceof CloudSpannerSQLException)
				code = ((CloudSpannerSQLException) exception).getCode();
			if (exception instanceof SpannerException)
				code = Code.forNumber(((SpannerException) exception).getCode());
			throw new CloudSpannerSQLException(
					(commit ? "Commit failed: " : "Rollback failed: ") + exception.getMessage(), code, exception);
		}
	}

}
