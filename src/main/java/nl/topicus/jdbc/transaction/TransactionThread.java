package nl.topicus.jdbc.transaction;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.common.base.Preconditions;
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

	enum TransactionStatus
	{
		NOT_STARTED, RUNNING, SUCCESS, FAIL;
	}

	private enum TransactionStopStatement
	{
		COMMIT, ROLLBACK, PREPARE, COMMIT_PREPARED, ROLLBACK_PREPARED;
	}

	private final Object monitor = new Object();

	private DatabaseClient dbClient;

	private boolean stop;

	private boolean stopped;

	private TransactionStatus status = TransactionStatus.NOT_STARTED;

	private Timestamp commitTimestamp;

	private Exception exception;

	private TransactionStopStatement stopStatement = null;

	/**
	 * The XA transaction id to be prepared/committed/rolled back
	 */
	private String xid;

	private final Set<String> stopStatementStrings = new HashSet<>(
			Arrays.asList(TransactionStopStatement.values()).stream().map(x -> x.name()).collect(Collectors.toList()));

	private List<Mutation> mutations = new ArrayList<>(40);

	private Map<Savepoint, Integer> savepoints = new HashMap<>();

	private BlockingQueue<Statement> statements = new LinkedBlockingQueue<>();

	private BlockingQueue<ResultSet> resultSets = new LinkedBlockingQueue<>();

	private static int threadInitNumber;

	private static synchronized int nextThreadNum()
	{
		return threadInitNumber++;
	}

	TransactionThread(DatabaseClient dbClient)
	{
		super("Google Cloud Spanner JDBC Transaction Thread-" + nextThreadNum());
		Preconditions.checkNotNull(dbClient, "dbClient may not be null");
		this.dbClient = dbClient;
		setDaemon(true);
	}

	@Override
	public void run()
	{
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
						status = TransactionStatus.RUNNING;
						while (!stop)
						{
							try
							{
								Statement statement = statements.poll(5, TimeUnit.SECONDS);
								if (statement != null)
								{
									String sql = statement.getSql();
									if (!stopStatementStrings.contains(sql))
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

						switch (stopStatement)
						{
						case COMMIT:
							transaction.buffer(mutations);
							break;
						case ROLLBACK:
							break;
						case PREPARE:
							XATransaction.prepareMutations(transaction, xid, mutations);
							break;
						case COMMIT_PREPARED:
							XATransaction.commitPrepared(transaction, xid);
							break;
						case ROLLBACK_PREPARED:
							XATransaction.rollbackPrepared(transaction, xid);
							break;
						}
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
				stopped = true;
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

	boolean hasBufferedMutations()
	{
		return !mutations.isEmpty();
	}

	int numberOfBufferedMutations()
	{
		return mutations.size();
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

	void setSavepoint(Savepoint savepoint)
	{
		Preconditions.checkNotNull(savepoint);
		savepoints.put(savepoint, mutations.size());
	}

	void rollbackSavepoint(Savepoint savepoint) throws CloudSpannerSQLException
	{
		Preconditions.checkNotNull(savepoint);
		Integer index = savepoints.get(savepoint);
		if (index == null)
		{
			throw new CloudSpannerSQLException("Unknown savepoint: " + savepoint.toString(), Code.INVALID_ARGUMENT);
		}
		mutations.subList(index.intValue(), mutations.size()).clear();
		removeSavepointsAfter(index.intValue());
	}

	void releaseSavepoint(Savepoint savepoint) throws CloudSpannerSQLException
	{
		Preconditions.checkNotNull(savepoint);
		Integer index = savepoints.get(savepoint);
		if (index == null)
		{
			throw new CloudSpannerSQLException("Unknown savepoint: " + savepoint.toString(), Code.INVALID_ARGUMENT);
		}
		removeSavepointsAfter(index.intValue());
	}

	private void removeSavepointsAfter(int index)
	{
		savepoints.entrySet().removeIf(e -> e.getValue() >= index);
	}

	Timestamp commit() throws SQLException
	{
		stopTransaction(TransactionStopStatement.COMMIT);
		return commitTimestamp;
	}

	void rollback() throws SQLException
	{
		stopTransaction(TransactionStopStatement.ROLLBACK);
	}

	void prepareTransaction(String xid) throws SQLException
	{
		this.xid = xid;
		stopTransaction(TransactionStopStatement.PREPARE);
	}

	void commitPreparedTransaction(String xid) throws SQLException
	{
		this.xid = xid;
		stopTransaction(TransactionStopStatement.COMMIT_PREPARED);
	}

	void rollbackPreparedTransaction(String xid) throws SQLException
	{
		this.xid = xid;
		stopTransaction(TransactionStopStatement.ROLLBACK_PREPARED);
	}

	private void stopTransaction(TransactionStopStatement statement) throws SQLException
	{
		if (status == TransactionStatus.FAIL || status == TransactionStatus.SUCCESS)
			return;
		while (status == TransactionStatus.NOT_STARTED)
		{
			try
			{
				Thread.sleep(1);
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
				throw new CloudSpannerSQLException(getFailedMessage(statement, e), Code.ABORTED, e);
			}
		}

		this.stopStatement = statement;
		stop = true;
		// Add a statement object in order to get the transaction thread to
		// proceed
		statements.add(Statement.of(statement.name()));
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
					throw new CloudSpannerSQLException(getFailedMessage(statement, e), Code.ABORTED, e);
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
			throw new CloudSpannerSQLException(getFailedMessage(statement, exception), code, exception);
		}
	}

	private String getFailedMessage(TransactionStopStatement statement, Exception e)
	{
		return statement.toString() + " failed: " + e.getMessage();
	}

	TransactionStatus getTransactionStatus()
	{
		return status;
	}

}
