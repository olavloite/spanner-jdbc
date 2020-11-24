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

  private enum StatementType {
    QUERY, UPDATE;
  }

  private static class GeneralStatement {
    private final Statement statement;
    private final StatementType type;

    private GeneralStatement(Statement statement, StatementType type) {
      this.statement = statement;
      this.type = type;
    }
  }

  private static class StatementResult {
    private final ResultSet resultSet;
    private final Long updateCount;
    private final RuntimeException exception;

    private static StatementResult of(ResultSet resultSet) {
      return new StatementResult(resultSet, null, null);
    }

    private static StatementResult of(Long updateCount) {
      return new StatementResult(null, updateCount, null);
    }

    private static StatementResult of(RuntimeException exception) {
      return new StatementResult(null, null, exception);
    }

    private StatementResult(ResultSet resultSet, Long updateCount, RuntimeException exception) {
      this.resultSet = resultSet;
      this.updateCount = updateCount;
      this.exception = exception;
    }
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

  private BlockingQueue<GeneralStatement> statements = new LinkedBlockingQueue<>();

  private BlockingQueue<StatementResult> statementResults = new LinkedBlockingQueue<>();

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

  @Override
  public void run() {
    TransactionRunner runner = dbClient.readWriteTransaction();
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
                GeneralStatement statement = statements.poll(5, TimeUnit.SECONDS);
                if (statement != null) {
                  String sql = statement.statement.getSql();
                  if (!stopStatementStrings.contains(sql)) {
                    try {
                      switch (statement.type) {
                        case QUERY:
                          statementResults.put(
                              StatementResult.of(transaction.executeQuery(statement.statement)));
                          break;
                        case UPDATE:
                          statementResults.put(
                              StatementResult.of(transaction.executeUpdate(statement.statement)));
                          break;
                        default:
                          throw new IllegalStateException(
                              "Unknown statement type: " + statement.type);
                      }
                    } catch (RuntimeException e) {
                      statementResults.put(StatementResult.of(e));
                      throw e;
                    }
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
      statements.put(new GeneralStatement(statement, StatementType.QUERY));
      StatementResult res = statementResults.take();
      if (res.exception != null) {
        throw res.exception;
      } else if (res.resultSet != null) {
        return res.resultSet;
      } else {
        throw new IllegalStateException("Statement did not return a resultset");
      }
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

  long executeUpdate(Statement statement) {
    try {
      statements.put(new GeneralStatement(statement, StatementType.UPDATE));
      StatementResult res = statementResults.take();
      if (res.exception != null) {
        throw res.exception;
      } else if (res.updateCount != null) {
        return res.updateCount;
      } else {
        throw new IllegalStateException("Statement did not return an update count");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new QueryException("Update execution interrupted", e);
    }
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
    statements.add(new GeneralStatement(Statement.of(statement.name()), StatementType.QUERY));
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
