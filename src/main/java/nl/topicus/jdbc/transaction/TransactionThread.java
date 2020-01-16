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
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options.QueryOption;
import com.google.cloud.spanner.Options.ReadOption;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.spanner.Type;
import com.google.common.base.Preconditions;
import com.google.rpc.Code;
import com.google.spanner.v1.ResultSetStats;
import nl.topicus.jdbc.CloudSpannerDriver;
import nl.topicus.jdbc.Logger;
import nl.topicus.jdbc.exception.CloudSpannerSQLException;

class TransactionThread extends Thread {
  public static class QueryException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private QueryException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static class RollbackException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }

  enum TransactionStatus {
    NOT_STARTED, RUNNING, SUCCESS, FAIL;
  }

  private enum TransactionStopStatement {
    COMMIT, ROLLBACK, PREPARE, COMMIT_PREPARED, ROLLBACK_PREPARED;
  }

  private final Logger logger;

  private final StackTraceElement[] stackTraceElements;

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

  private final Set<String> stopStatementStrings =
      new HashSet<>(Arrays.asList(TransactionStopStatement.values()).stream().map(x -> x.name())
          .collect(Collectors.toList()));

  private List<Mutation> mutations = new ArrayList<>(40);

  private Map<Savepoint, Integer> savepoints = new HashMap<>();

  private BlockingQueue<Statement> statements = new LinkedBlockingQueue<>();

  private BlockingQueue<ResultSet> resultSets = new LinkedBlockingQueue<>();

  private static int threadInitNumber;

  private static synchronized int nextThreadNum() {
    return threadInitNumber++;
  }

  TransactionThread(DatabaseClient dbClient, Logger logger) {
    super("Google Cloud Spanner JDBC Transaction Thread-" + nextThreadNum());
    Preconditions.checkNotNull(dbClient, "dbClient may not be null");
    Preconditions.checkNotNull(logger, "logger may not be null");
    this.dbClient = dbClient;
    this.logger = logger;
    if (logger != null && logger.logDebug()) {
      this.stackTraceElements = Thread.currentThread().getStackTrace();
    } else {
      this.stackTraceElements = null;
    }
    setDaemon(true);
  }

  private static final class FailedResultSet implements ResultSet {
    private final SpannerException e;

    private FailedResultSet(SpannerException e) {
      this.e = e;
    }

    @Override
    public Type getType() {
      throw e;
    }

    @Override
    public int getColumnCount() {
      throw e;
    }

    @Override
    public int getColumnIndex(String columnName) {
      throw e;
    }

    @Override
    public Type getColumnType(int columnIndex) {
      throw e;
    }

    @Override
    public Type getColumnType(String columnName) {
      throw e;
    }

    @Override
    public boolean isNull(int columnIndex) {
      throw e;
    }

    @Override
    public boolean isNull(String columnName) {
      throw e;
    }

    @Override
    public boolean getBoolean(int columnIndex) {
      throw e;
    }

    @Override
    public boolean getBoolean(String columnName) {
      throw e;
    }

    @Override
    public long getLong(int columnIndex) {
      throw e;
    }

    @Override
    public long getLong(String columnName) {
      throw e;
    }

    @Override
    public double getDouble(int columnIndex) {
      throw e;
    }

    @Override
    public double getDouble(String columnName) {
      throw e;
    }

    @Override
    public String getString(int columnIndex) {
      throw e;
    }

    @Override
    public String getString(String columnName) {
      throw e;
    }

    @Override
    public ByteArray getBytes(int columnIndex) {
      throw e;
    }

    @Override
    public ByteArray getBytes(String columnName) {
      throw e;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
      throw e;
    }

    @Override
    public Timestamp getTimestamp(String columnName) {
      throw e;
    }

    @Override
    public Date getDate(int columnIndex) {
      throw e;
    }

    @Override
    public Date getDate(String columnName) {
      throw e;
    }

    @Override
    public boolean[] getBooleanArray(int columnIndex) {
      throw e;
    }

    @Override
    public boolean[] getBooleanArray(String columnName) {
      throw e;
    }

    @Override
    public List<Boolean> getBooleanList(int columnIndex) {
      throw e;
    }

    @Override
    public List<Boolean> getBooleanList(String columnName) {
      throw e;
    }

    @Override
    public long[] getLongArray(int columnIndex) {
      throw e;
    }

    @Override
    public long[] getLongArray(String columnName) {
      throw e;
    }

    @Override
    public List<Long> getLongList(int columnIndex) {
      throw e;
    }

    @Override
    public List<Long> getLongList(String columnName) {
      throw e;
    }

    @Override
    public double[] getDoubleArray(int columnIndex) {
      throw e;
    }

    @Override
    public double[] getDoubleArray(String columnName) {
      throw e;
    }

    @Override
    public List<Double> getDoubleList(int columnIndex) {
      throw e;
    }

    @Override
    public List<Double> getDoubleList(String columnName) {
      throw e;
    }

    @Override
    public List<String> getStringList(int columnIndex) {
      throw e;
    }

    @Override
    public List<String> getStringList(String columnName) {
      throw e;
    }

    @Override
    public List<ByteArray> getBytesList(int columnIndex) {
      throw e;
    }

    @Override
    public List<ByteArray> getBytesList(String columnName) {
      throw e;
    }

    @Override
    public List<Timestamp> getTimestampList(int columnIndex) {
      throw e;
    }

    @Override
    public List<Timestamp> getTimestampList(String columnName) {
      throw e;
    }

    @Override
    public List<Date> getDateList(int columnIndex) {
      throw e;
    }

    @Override
    public List<Date> getDateList(String columnName) {
      throw e;
    }

    @Override
    public List<Struct> getStructList(int columnIndex) {
      throw e;
    }

    @Override
    public List<Struct> getStructList(String columnName) {
      throw e;
    }

    @Override
    public boolean next() throws SpannerException {
      throw e;
    }

    @Override
    public Struct getCurrentRowAsStruct() {
      throw e;
    }

    @Override
    public void close() {}

    @Override
    public ResultSetStats getStats() {
      throw e;
    }
  }

  private static final class FailedTransactionContext implements TransactionContext {
    private final SpannerException e;
    private final FailedResultSet rs;

    private FailedTransactionContext(SpannerException e) {
      this.e = e;
      this.rs = new FailedResultSet(e);
    }

    @Override
    public ResultSet read(String table, KeySet keys, Iterable<String> columns,
        ReadOption... options) {
      return rs;
    }

    @Override
    public ResultSet readUsingIndex(String table, String index, KeySet keys,
        Iterable<String> columns, ReadOption... options) {
      return rs;
    }

    @Override
    public Struct readRow(String table, Key key, Iterable<String> columns) {
      throw e;
    }

    @Override
    public Struct readRowUsingIndex(String table, String index, Key key, Iterable<String> columns) {
      throw e;
    }

    @Override
    public ResultSet executeQuery(Statement statement, QueryOption... options) {
      return rs;
    }

    @Override
    public ResultSet analyzeQuery(Statement statement, QueryAnalyzeMode queryMode) {
      return rs;
    }

    @Override
    public void close() {}

    @Override
    public void buffer(Mutation mutation) {}

    @Override
    public void buffer(Iterable<Mutation> mutations) {}
  }

  private static final class FailedTransactionRunner implements TransactionRunner {
    private final SpannerException e;

    private FailedTransactionRunner(SpannerException e) {
      this.e = e;
    }

    @Override
    public <T> T run(TransactionCallable<T> callable) {
      try {
        return callable.run(new FailedTransactionContext(e));
      } catch (Exception e) {
        throw SpannerExceptionFactory.newSpannerException(e);
      }
    }

    @Override
    public Timestamp getCommitTimestamp() {
      throw e;
    }
  }

  @Override
  public void run() {
    TransactionRunner runner = null;
    try {
      runner = dbClient.readWriteTransaction();
    } catch (SpannerException e) {
      runner = new FailedTransactionRunner(e);
    }
    synchronized (monitor) {
      try {
        status = runner.run(new TransactionCallable<TransactionStatus>() {

          @Override
          public TransactionStatus run(TransactionContext transaction) throws Exception {
            long startTime = System.currentTimeMillis();
            long lastTriggerTime = startTime;
            boolean transactionStartedLogged = false;
            boolean stackTraceLoggedForKeepAlive = false;
            boolean stackTraceLoggedForLongRunning = false;
            status = TransactionStatus.RUNNING;
            while (!stop) {
              try {
                Statement statement = statements.poll(5, TimeUnit.SECONDS);
                if (statement != null) {
                  String sql = statement.getSql();
                  if (!stopStatementStrings.contains(sql)) {
                    resultSets.put(transaction.executeQuery(statement));
                  }
                } else {
                  // keep alive
                  transactionStartedLogged =
                      logTransactionStarted(transactionStartedLogged, startTime);
                  logger.info(String.format("%s, %s", getName(),
                      "Transaction has been inactive for more than 5 seconds and will do a keep-alive query"));
                  if (!stackTraceLoggedForKeepAlive) {
                    logStartStackTrace();
                    stackTraceLoggedForKeepAlive = true;
                  }
                  try (ResultSet rs = transaction.executeQuery(Statement.of("SELECT 1"))) {
                    rs.next();
                  }
                }
                if (!stop && logger.logInfo()
                    && (System.currentTimeMillis() - lastTriggerTime) > CloudSpannerDriver
                        .getLongTransactionTrigger()) {
                  transactionStartedLogged =
                      logTransactionStarted(transactionStartedLogged, startTime);
                  logger.info(String.format("%s, %s", getName(), "Transaction has been running for "
                      + (System.currentTimeMillis() - startTime) + "ms"));
                  if (!stackTraceLoggedForLongRunning) {
                    logStartStackTrace();
                    stackTraceLoggedForLongRunning = true;
                  }
                  lastTriggerTime = System.currentTimeMillis();
                }
              } catch (InterruptedException e) {
                logDebugIfTransactionStartedLogged(transactionStartedLogged,
                    "Transaction interrupted");
                stopped = true;
                exception = e;
                throw e;
              }
            }

            switch (stopStatement) {
              case COMMIT:
                logDebugIfTransactionStartedLogged(transactionStartedLogged,
                    "Transaction committed");
                transaction.buffer(mutations);
                break;
              case ROLLBACK:
                // throw an exception to force a rollback
                logDebugIfTransactionStartedLogged(transactionStartedLogged,
                    "Transaction rolled back");
                throw new RollbackException();
              case PREPARE:
                logDebugIfTransactionStartedLogged(transactionStartedLogged,
                    "Transaction prepare called");
                XATransaction.prepareMutations(transaction, xid, mutations);
                break;
              case COMMIT_PREPARED:
                logDebugIfTransactionStartedLogged(transactionStartedLogged,
                    "Transaction commit prepared called");
                XATransaction.commitPrepared(transaction, xid);
                break;
              case ROLLBACK_PREPARED:
                logDebugIfTransactionStartedLogged(transactionStartedLogged,
                    "Transaction rollback prepared called");
                XATransaction.rollbackPrepared(transaction, xid);
                break;
            }
            logDebugIfTransactionStartedLogged(transactionStartedLogged,
                "Transaction successfully stopped");
            return TransactionStatus.SUCCESS;
          }
        });
        commitTimestamp = runner.getCommitTimestamp();
      } catch (Exception e) {
        if (e.getCause() instanceof RollbackException) {
          status = TransactionStatus.SUCCESS;
        } else {
          // if statement prevents unnecessary String.format(...) call
          if (logger.logDebug()) {
            logger.debug(String.format("%s, %s", getName(),
                "Transaction threw an exception: " + e.getMessage()));
          }
          status = TransactionStatus.FAIL;
          exception = e;
        }
      } finally {
        stopped = true;
        monitor.notifyAll();
      }
    }
  }

  private void logDebugIfTransactionStartedLogged(boolean transactionStartedLogged, String log) {
    if (transactionStartedLogged) {
      logger.debug(String.format("%s, %s", getName(), log));
    }
  }

  private boolean logTransactionStarted(boolean transactionStartedLogged, long startTime) {
    if (!transactionStartedLogged) {
      logger.debug(String.format("%s, %s", getName(),
          "This transaction started at " + new java.sql.Timestamp(startTime).toString()));
    }
    return true;
  }

  private void logStartStackTrace() {
    if (stackTraceElements != null) {
      logger.debug(String.format("%s, %s", getName(), "Transaction was started by: "));
      for (StackTraceElement ste : stackTraceElements) {
        logger.debug("\t" + ste.toString());
      }
    }
  }

  ResultSet executeQuery(Statement statement) {
    try {
      statements.put(statement);
      return resultSets.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new QueryException("Query execution interrupted", e);
    }
  }

  boolean hasBufferedMutations() {
    return !mutations.isEmpty();
  }

  int numberOfBufferedMutations() {
    return mutations.size();
  }

  void buffer(Mutation mutation) {
    if (mutation == null)
      throw new NullPointerException("Mutation is null");
    mutations.add(mutation);
  }

  void buffer(Iterable<Mutation> mutations) {
    Iterator<Mutation> it = mutations.iterator();
    while (it.hasNext())
      buffer(it.next());
  }

  void setSavepoint(Savepoint savepoint) {
    Preconditions.checkNotNull(savepoint);
    savepoints.put(savepoint, mutations.size());
  }

  void rollbackSavepoint(Savepoint savepoint) throws CloudSpannerSQLException {
    Preconditions.checkNotNull(savepoint);
    Integer index = savepoints.get(savepoint);
    if (index == null) {
      throw new CloudSpannerSQLException("Unknown savepoint: " + savepoint.toString(),
          Code.INVALID_ARGUMENT);
    }
    mutations.subList(index.intValue(), mutations.size()).clear();
    removeSavepointsAfter(index.intValue());
  }

  void releaseSavepoint(Savepoint savepoint) throws CloudSpannerSQLException {
    Preconditions.checkNotNull(savepoint);
    Integer index = savepoints.get(savepoint);
    if (index == null) {
      throw new CloudSpannerSQLException("Unknown savepoint: " + savepoint.toString(),
          Code.INVALID_ARGUMENT);
    }
    removeSavepointsAfter(index.intValue());
  }

  private void removeSavepointsAfter(int index) {
    savepoints.entrySet().removeIf(e -> e.getValue() >= index);
  }

  Timestamp commit() throws SQLException {
    stopTransaction(TransactionStopStatement.COMMIT);
    return commitTimestamp;
  }

  void rollback() throws SQLException {
    stopTransaction(TransactionStopStatement.ROLLBACK);
  }

  void prepareTransaction(String xid) throws SQLException {
    this.xid = xid;
    stopTransaction(TransactionStopStatement.PREPARE);
  }

  void commitPreparedTransaction(String xid) throws SQLException {
    this.xid = xid;
    stopTransaction(TransactionStopStatement.COMMIT_PREPARED);
  }

  void rollbackPreparedTransaction(String xid) throws SQLException {
    this.xid = xid;
    stopTransaction(TransactionStopStatement.ROLLBACK_PREPARED);
  }

  private void stopTransaction(TransactionStopStatement statement) throws SQLException {
    if (status == TransactionStatus.FAIL || status == TransactionStatus.SUCCESS)
      return;
    while (status == TransactionStatus.NOT_STARTED) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CloudSpannerSQLException(getFailedMessage(statement, e), Code.ABORTED, e);
      }
    }

    this.stopStatement = statement;
    stop = true;
    // Add a statement object in order to get the transaction thread to
    // proceed
    statements.add(Statement.of(statement.name()));
    synchronized (monitor) {
      while (!stopped || status == TransactionStatus.NOT_STARTED
          || status == TransactionStatus.RUNNING) {
        try {
          monitor.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new CloudSpannerSQLException(getFailedMessage(statement, e), Code.ABORTED, e);
        }
      }
    }
    if (status == TransactionStatus.FAIL && exception != null) {
      Code code = Code.UNKNOWN;
      if (exception instanceof CloudSpannerSQLException)
        code = ((CloudSpannerSQLException) exception).getCode();
      if (exception instanceof SpannerException)
        code = Code.forNumber(((SpannerException) exception).getCode());
      throw new CloudSpannerSQLException(getFailedMessage(statement, exception), code, exception);
    }
  }

  private String getFailedMessage(TransactionStopStatement statement, Exception e) {
    return statement.toString() + " failed: " + e.getMessage();
  }

  TransactionStatus getTransactionStatus() {
    return status;
  }

}
