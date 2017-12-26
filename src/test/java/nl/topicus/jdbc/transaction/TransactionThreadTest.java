package nl.topicus.jdbc.transaction;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.BiFunction;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.internal.stubbing.answers.Returns;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.TransactionRunner;

import nl.topicus.jdbc.test.category.UnitTest;
import nl.topicus.jdbc.transaction.TransactionThread.TransactionStatus;

@Category(UnitTest.class)
public class TransactionThreadTest
{

	private class MockTransactionRunner implements TransactionRunner
	{
		private final int runs;

		private Timestamp commitTimestamp;

		private TransactionContextMock mock = new TransactionContextMock();

		private MockTransactionRunner()
		{
			this(1);
		}

		private MockTransactionRunner(int runs)
		{
			this.runs = runs;
		}

		@Override
		public <T> T run(TransactionCallable<T> callable)
		{
			T res = null;
			try
			{
				for (int i = 0; i < runs; i++)
				{
					mock.clearMutations();
					res = callable.run(mock.createTransactionContextMock());
				}
				commitTimestamp = Timestamp.now();
			}
			catch (Exception e)
			{
				throw new RuntimeException(e.getMessage(), e);
			}
			return res;
		}

		@Override
		public Timestamp getCommitTimestamp()
		{
			return commitTimestamp;
		}

	}

	@FunctionalInterface
	private static interface ConsumerWithSQLException<T>
	{
		public void consume(T t) throws SQLException;
	}

	private static class TestSubject
	{
		private final TransactionThread thread;

		private final TransactionContextMock transaction;

		private TestSubject(TransactionThread thread, TransactionContextMock transaction)
		{
			this.thread = thread;
			this.transaction = transaction;
		}
	}

	private TestSubject createTestSubject()
	{
		MockTransactionRunner runner = new MockTransactionRunner();
		DatabaseClient dbClient = mock(DatabaseClient.class);
		when(dbClient.readWriteTransaction()).then(new Returns(runner));
		return new TestSubject(new TransactionThread(dbClient), runner.mock);
	}

	@Test
	public void testRunSimpleCommit() throws SQLException
	{
		testRunAction(t -> t.commit());
	}

	@Test
	public void testRunMultipleCommit() throws SQLException
	{
		testRunAction(t -> t.commit(), 3);
	}

	@Test
	public void testRunSimpleRollback() throws SQLException
	{
		testRunAction(t -> t.rollback());
	}

	@Test
	public void testRunMultipleRollback() throws SQLException
	{
		testRunAction(t -> t.rollback(), 3);
	}

	@Test
	public void testRunSimplePrepare() throws SQLException
	{
		testRunAction(t -> t.prepareTransaction(TransactionContextMock.XID_WITHOUT_MUTATIONS.toString()));
	}

	@Test
	public void testRunMultiplePrepare() throws SQLException
	{
		testRunAction(t -> t.prepareTransaction(TransactionContextMock.XID_WITHOUT_MUTATIONS.toString()), 3);
	}

	@Test
	public void testRunSimpleCommitPrepared() throws SQLException
	{
		testRunAction(t -> t.commitPreparedTransaction(TransactionContextMock.XID_WITHOUT_MUTATIONS.toString()));
	}

	@Test
	public void testRunMultipleCommitPrepared() throws SQLException
	{
		testRunAction(t -> t.commitPreparedTransaction(TransactionContextMock.XID_WITHOUT_MUTATIONS.toString()), 3);
	}

	@Test
	public void testRunSimpleRollbackPrepared() throws SQLException
	{
		testRunAction(t -> t.rollbackPreparedTransaction(TransactionContextMock.XID_WITHOUT_MUTATIONS.toString()));
	}

	@Test
	public void testRunMultipleRollbackPrepared() throws SQLException
	{
		testRunAction(t -> t.rollbackPreparedTransaction(TransactionContextMock.XID_WITHOUT_MUTATIONS.toString()), 3);
	}

	private void testRunAction(ConsumerWithSQLException<TransactionThread> action) throws SQLException
	{
		testRunAction(action, null);
	}

	private void testRunAction(ConsumerWithSQLException<TransactionThread> action, int runs) throws SQLException
	{
		testRunAction(action, runs, null);
	}

	private void testRunAction(ConsumerWithSQLException<TransactionThread> action,
			BiFunction<TransactionThreadTest, TransactionThread, Integer> callback) throws SQLException
	{
		testRunAction(action, 1, callback);
	}

	private void testRunAction(ConsumerWithSQLException<TransactionThread> action, int runs,
			BiFunction<TransactionThreadTest, TransactionThread, Integer> callback) throws SQLException
	{
		// 10 test runs to minimize the chance of missing errors caused by
		// concurrency issues
		try
		{
			for (int i = 0; i < 10; i++)
			{
				TestSubject subject = createTestSubject();
				TransactionThread thread = subject.thread;
				thread.start();
				int expectedNumberOfMutations = 0;
				if (callback != null)
				{
					expectedNumberOfMutations = callback.apply(this, thread);
				}
				action.consume(thread);
				assertEquals(TransactionStatus.SUCCESS, thread.getTransactionStatus());
				assertEquals(expectedNumberOfMutations, subject.transaction.getMutations().size());
			}
		}
		catch (Exception e)
		{
			throw new SQLException("Exception during test: " + e.getMessage(), e);
		}
	}

	@Test
	public void testRunCommitWithMutations() throws SQLException
	{
		testRunAction(t -> t.commit(), (test, subject) -> test.testRunWithMutationsCallback(subject, 3));
	}

	@Test
	public void testRunMultipleCommitWithMutations() throws SQLException
	{
		testRunAction(t -> t.commit(), 3, (test, subject) -> test.testRunWithMutationsCallback(subject, 3));
	}

	private int testRunWithMutationsCallback(TransactionThread subject, int expectedNumberOfMutations)
	{
		createThreeMutations(subject);
		return expectedNumberOfMutations;
	}

	private void createThreeMutations(TransactionThread subject)
	{
		subject.buffer(Mutation.newInsertBuilder("FOO").set("ID").to(1L).build());
		subject.buffer(Arrays.asList(Mutation.newInsertBuilder("FOO").set("ID").to(2L).build(),
				Mutation.newInsertBuilder("FOO").set("ID").to(3L).build()));
	}

	@Test
	public void testRunRollbackWithMutations() throws SQLException
	{
		testRunAction(t -> t.rollback(), (test, subject) -> test.testRunWithMutationsCallback(subject, 0));
	}

	@Test
	public void testRunMultipleRollbackWithMutations() throws SQLException
	{
		testRunAction(t -> t.rollback(), 3, (test, subject) -> test.testRunWithMutationsCallback(subject, 0));
	}

	@Test
	public void testRunPrepareWithMutations() throws SQLException
	{
		testRunAction(t -> t.prepareTransaction(TransactionContextMock.XID_WITH_MUTATIONS.toString()),
				(test, subject) -> test.testRunWithMutationsCallback(subject, 3));
	}

	@Test
	public void testRunMultiplePrepareWithMutations() throws SQLException
	{
		testRunAction(t -> t.prepareTransaction(TransactionContextMock.XID_WITH_MUTATIONS.toString()), 3,
				(test, subject) -> test.testRunWithMutationsCallback(subject, 3));
	}

	@Test
	public void testRunCommitPreparedWithMutations() throws SQLException
	{
		// Apply 3 mutations and delete 3 prepared mutations. The delete is
		// performed in one mutation with a key set containing 3 keys.
		testRunAction(t -> t.commitPreparedTransaction(TransactionContextMock.XID_WITH_MUTATIONS.toString()),
				(test, subject) -> test.testRunWithMutationsCallback(subject, 4));
	}

	@Test
	public void testRunMultipleCommitPreparedWithMutations() throws SQLException
	{
		// Apply 3 mutations and delete 3 prepared mutations. The delete is
		// performed in one mutation with a key set containing 3 keys.
		testRunAction(t -> t.commitPreparedTransaction(TransactionContextMock.XID_WITH_MUTATIONS.toString()), 2,
				(test, subject) -> test.testRunWithMutationsCallback(subject, 4));
	}

	@Test
	public void testRunRollbackPreparedWithMutations() throws SQLException
	{
		// Apply 0 mutations and delete 3 prepared mutations. The delete is
		// performed in one mutation with a key set containing 3 keys.
		testRunAction(t -> t.rollbackPreparedTransaction(TransactionContextMock.XID_WITH_MUTATIONS.toString()),
				(test, subject) -> test.testRunWithMutationsCallback(subject, 1));
	}

	@Test
	public void testRunMultipleRollbackPreparedWithMutations() throws SQLException
	{
		// Apply 0 mutations and delete 3 prepared mutations. The delete is
		// performed in one mutation with a key set containing 3 keys.
		testRunAction(t -> t.rollbackPreparedTransaction(TransactionContextMock.XID_WITH_MUTATIONS.toString()), 2,
				(test, subject) -> test.testRunWithMutationsCallback(subject, 1));
	}

}
