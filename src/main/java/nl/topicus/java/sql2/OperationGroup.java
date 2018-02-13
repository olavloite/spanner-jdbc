/*
 * Copyright (c)  2017, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */
package nl.topicus.java.sql2;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * A set of {@link Operation}s that share certain properties, are managed as a
 * unit, and are executed as a unit. The {@link Operation}s created by an
 * {@link OperationGroup} and submitted are the member {@link Operation}s of
 * that {@link OperationGroup}.
 *
 * An {@link OperationGroup} conceptually has a collection of member
 * {@link Operation}s. When an {@link OperationGroup} is submitted it is placed
 * in the collection of the {@link OperationGroup} of which it is a member. The
 * member {@link OperationGroup} is executed according to the attributes of the
 * {@link OperationGroup} of which it is a member. The member {@link Operation}s
 * of an {@link OperationGroup} are executed according to the attributes of that
 * {@link OperationGroup}.
 *
 * How an {@link OperationGroup} is executed depends on its attributes.
 *
 * If an {@link OperationGroup} has a condition and the value of that condition
 * is {@link Boolean#TRUE} then execute the member {@link Operation}s as below.
 * If it is {@link Boolean#FALSE} then the {@link OperationGroup} is completed
 * with the value null. If the condition completed exceptionally then the
 * {@link OperationGroup} is completed exceptionally with
 * {@link SqlSkippedException} that has that exception as its cause.
 *
 * If the {@link OperationGroup} is sequential the member {@link Operation}s are
 * executed in the order they were submitted. If it is parallel, they may be
 * executed in any order including simultaneously.
 *
 * If an {@link OperationGroup} is dependent and a member {@link Operation}
 * completes exceptionally the remaining member {@link Operation}s in the
 * collection are completed exceptionally with a {@link SqlSkippedException}
 * that has the initial {@link Exception} as its cause. A member
 * {@link Operation} in-flight may either complete normally or be completed
 * exceptionally but must complete one way or the other. [NOTE: Too strong?]
 *
 * If an {@link OperationGroup} is held additional member {@link Operation}s may
 * be submitted after the {@link OperationGroup} is submitted. If an
 * {@link OperationGroup} is not held, no additional member {@link Operation}s
 * may be submitted after the {@link OperationGroup} is submitted. If an
 * {@link OperationGroup} is held it will be completed only after it is released
 * or if conditional and the condition is not {@link Boolean#TRUE}. If a
 * {@link OperationGroup} is dependent, held, one of its member
 * {@link Operation}s completed exceptionally, and its queue is empty then the
 * {@link OperationGroup} is released.
 *
 * ISSUE: Currently no way to create a nested {@link OperationGroup}. That is a
 * limitation but may be a simplification we can live with. Or not.
 *
 * @param <S> The type of the result of the member {@link Operation}s
 * @param <T> The type of the result of aggregating the member
 * {@link Operation}s
 */
public interface OperationGroup<S, T> extends Operation<T> {

  /**
   * Mark this {@link OperationGroup} as parallel. If this method is not called
   * the {@link OperationGroup} is sequential. If an {@link OperationGroup} is
   * parallel, member {@link Operation}s may be executed in any order including
   * in parallel. If an {@link OperationGroup} is sequential, the default,
   * member {@link Operation}s are executed strictly in the order they are
   * submitted.
   *
   * Note: There is no covariant override of this method in {@link Connection}
   * as there is only a small likelihood of needing it.
   *
   * @return this {@link OperationGroup}
   * @throws IllegalStateException if this method has been submitted or any
   * member {@link Operation}s have been created.
   */
  public OperationGroup<S, T> parallel();

  /**
   * Mark this {@link OperationGroup} as independent. If this method is not
   * called the {@link OperationGroup} is dependent, the default. If an
   * {@link OperationGroup} is independent then failure of one member
   * {@link Operation} does not affect the execution of other member
   * {@link Operation}s. If an {@link OperationGroup} is dependent then failure
   * of one member {@link Operation} will cause all member {@link Operation}s
   * remaining in the queue to be completed exceptionally with a
   * {@link SqlSkippedException} with the cause set to the original exception.
   *
   * Note: There is no covariant override of this method in {@link Connection}
   * as there is only a small likelihood of needing it.
   *
   * @return this {@link OperationGroup}
   * @throws IllegalStateException if this {@link OperationGroup} has been
   * submitted or any member {@link Operation}s have been created
   */
  public OperationGroup<S, T> independent();

  /**
   * Define a condition that determines whether the member {@link Operation}s of
   * this {@link OperationGroup} are executed or not. If and when this
   * {@link OperationGroup} is executed then if the condition argument is
   * completed with {@link Boolean#TRUE} the member {@link Operation}s are
   * executed. If {@link Boolean#FALSE} or if it is completed exceptionally the
   * member {@link Operation}s are not executed but are removed from the queue.
   * After all member {@link Operation}s have been removed from the queue this
   * {@link OperationGroup} is completed with {@code null}.
   *
   * Note: There is no covariant override of this method in Connection as there
   * is only a small likelihood of needing it.
   *
   * ISSUE: Should the member Operations be skipped or otherwise completed
   * exceptionally?
   *
   * @param condition a {@link CompletableFuture} the value of which determines whether
   * this {@link OperationGroup} is executed or not
   * @return this OperationGroup
   * @throws IllegalStateException if this {@link OperationGroup} has been
   * submitted or any member {@link Operation}s have been created
   */
  public OperationGroup<S, T> conditional(CompletableFuture<Boolean> condition);

  /**
   * Mark this {@link OperationGroup} as held. It can be executed but cannot be
   * completed. A {@link OperationGroup} that is held remains in the queue even
   * if all of its current member {@link Operation}s have completed. So long as
   * the {@link OperationGroup} is held new member {@link Operation}s can be
   * submitted. A {@link OperationGroup} that is held must be released before it
   * can be completed and removed from the queue.
   *
   * Note: There is no covariant override of this method in Connection as there
   * is only a small likelihood of needing it.
   *
   * ISSUE: Need a better name.
   *
   * @return this OperationGroup
   * @throws IllegalStateException if this {@link OperationGroup} has been
   * submitted
   */
  public OperationGroup<S, T> holdForMoreMembers();

  /**
   * Allow this {@link OperationGroup} to be completed and removed from the
   * queue once all of its member {@link Operation}s have been completed. After
   * this method is called no additional member {@link Operation}s can be
   * submitted. Once all member {@link Operation}s have been removed from the
   * queue this {@link OperationGroup} will be completed and removed from the
   * queue.
   *
   * Calling this method when this {@link OperationGroup} is not held is a noop.
   *
   * Note: There is no covariant override of this method in Connection as there
   * is only a small likelihood of needing it.
   *
   * ISSUE: Need a better name.
   *
   * @return this OperationGroup
   * @throws IllegalStateException if this {@link OperationGroup} has been
   * completed
   */
  public OperationGroup<S, T> releaseProhibitingMoreMembers();

  /**
   * Supplier of the initial value provided to {@link memberAggregator}. The
   * default value is {@code () -&gt; null}.
   *
   * @param supplier provides the initial value for the {@link memberAggregator}
   * @return this {@link OperationGroup}
   * @throws IllegalStateException if called more than once or if this
   * {@link OperationGroup} has been submitted
   */
  public OperationGroup<S, T> initialValue(Supplier<T> supplier);

  /**
   * Function that aggregates the results of the member {@link Operation}s.
   * Called once for each member {@link Operation} that completes normally. The
   * first argument of the first call is the value supplied by
   * {@link initialValue} {@code supplier}. For; subsequent calls it is the
   * value returned by the previous call. If this {@link OperationGroup} is
   * sequential the values are passed to the function in the order the member
   * {@link Operation}s complete. If this {@link OperationGroup} is parallel,
   * values may be passed to the function in any order though an approximation
   * of the order in which they complete is recommended. The default value is 
   * {@code (a, b) -&gt; null}.
   *
   * @param aggregator a {@link BiFunction} that aggregates the results of the
   * member {@link Operation}s
   * @return this {@link OperationGroup}
   * @throws IllegalStateException if called more than once or if this
   * {@link OperationGroup} has been submitted
   */
  public OperationGroup<S, T> memberAggregator(BiFunction<T, S, T> aggregator);

  /**
   * Return a new {@link BatchCountOperation}.
   *
   * @param <R> the result type of the returned {@link BatchCountOperation}
   * @param sql SQL to be executed. Must return an update count.
   * @return a new {@link BatchCountOperation} that is a member of this 
   * {@link OperationGroup}
   */
  public <R extends S> BatchCountOperation<R> batchCountOperation(String sql);

  /**
   * Return a new {@link CountOperation}.
   *
   * @param <R> the result type of the returned {@link CountOperation}
   * @param sql SQL to be executed. Must return an update count.
   * @return an new {@link CountOperation} that is a member of this 
   * {@link OperationGroup}
   *
   */
  public <R extends S> ParameterizedCountOperation<R> countOperation(String sql);

  /**
   * Return a new {@link Operation} for a SQL that doesn't return any result,
   * for example DDL.
   *
   * @param sql SQL for the {@link Operation}.
   * @return a new {@link Operation} that is a member of this 
   * {@link OperationGroup}
   */
  public Operation<Void> operation(String sql);

  /**
   * Return a new {@link OutOperation}. The SQL must return a set of zero or
   * more out parameters or function results.
   *
   * @param <R> the result type of the retuned {@link OutOperation}
   * @param sql SQL for the {@link Operation}. Must return zero or more out
   * parameters or function results.
   * @return a new {@link OutOperation} that is a member of this 
   * {@link OperationGroup}
   */
  public <R extends S> OutOperation<R> outOperation(String sql);

  /**
   * Return a {@link ParameterizedRowOperation}.
   *
   * @param <R> the type of the result of the returned {@link ParameterizedRowOperation}
   * @param sql SQL for the {@link Operation}. Must return a row sequence.
   * @return a new {@link ParameterizedRowOperation} that is a member of this 
   * {@link OperationGroup}
   */
  public <R extends S> ParameterizedRowOperation<R> rowOperation(String sql);

  /**
   * Return a {@link StaticMultiOperation}.
   *
   * @param <R> the type of the result of the returned 
   * {@link StaticMultiOperation}
   * @param sql SQL for the {@link Operation}
   * @return a new {@link StaticMultiOperation} that is a member of this 
   * {@link OperationGroup}
   */
  public <R extends S> StaticMultiOperation<R> staticMultiOperation(String sql);
  
  public <R extends S> PublisherOperation<R> publisherOperation(String sql);

  /**
   * Return a {@link DynamicMultiOperation}. Use this when the number and type
   * of the results is not knowable.
   *
   * @param <R> the type of the result of the returned 
   * {@link DynamicMultiOperation}
   * @param sql SQL for the {@link Operation}
   * @return a new {@link DynamicMultiOperation} that is a member of this
   * {@link OperationGroup}
   */
  public <R extends S> DynamicMultiOperation<R> dynamicMultiOperation(String sql);

  /**
   * Return an {@link Operation} that ends the current database transaction.
   * After submitting this {@link Operation} there is no current
   * {@link Transaction}. The transaction is ended with a commit unless the
   * {@link Transaction} has been {@link Transaction#setRollbackOnly} in which
   * case the transaction is ended with a rollback.
   *
   * If an {@link OperationGroup} has this as a member, the type argument
   * {@link S} of that {@link OperationGroup} must be a supertype of
   * {@link TransactionOutcome}.
   *
   * @return an Operation that will end the current transaction. This Operation
   * will end the transaction as specified by the {@link Transaction} that was
   * current when this Operation was submitted.
   * @throws IllegalStateException if this {@link OperationGroup} is parallel.
   */
  public Operation<TransactionOutcome> commitOperation();

  /**
   * Convenience method that creates and submits a commit {@link Operation}.
   * @return this {@link OperationGroup}
   */
  public default OperationGroup<S, T> commit() {
    this.commitOperation().submit();
    return this;
  }

  /**
   * Return an {@link Operation} that rollsback the current database
   * transaction. After submitting this Operation there is no current
   * {@link Transaction}. The transaction is ended with a rollback.
   *
   * If an {@link OperationGroup} has this as a member, the type argument
   * {@link S} of that {@link OperationGroup} must be a supertype of
   * {@link TransactionOutcome}.
   *
   * @return the {@link Submission} for an {@link Operation} that will always
   * rollback the current database transaction.
   * @throws IllegalStateException if this {@link OperationGroup} is parallel.
   */
  public Operation<TransactionOutcome> rollbackOperation();

  /**
   * Create a rollback {@link Operation} and submit it. Convenience method.
   *
   * @return this {@link OperationGroup}
   */
  public default OperationGroup<S, T> rollback() {
    this.rollbackOperation().submit();
    return this;
  }

  /**
   * Return a Runnable Operation
   *
   * @return a LocalOperation
   * @throws IllegalStateException if this OperationGroup has been submitted and
   * is not held
   */
  public LocalOperation<T> localOperation();
  
  /**
   * Provide a {@link Flow.Publisher} that will stream {@link Operation}s to this
   * {@link OperationGroup}. Use of this method is optional. Any
   * {@link Operation} passed to {@link Flow.Subscriber#onNext} must be created by
   * this {@link OperationGroup}. If it is not {@link Flow.Subscriber#onNext} throws
   * {@link IllegalArgumentException}. {@link Flow.Subscriber#onNext} submits the
   * {@link Operation} argument, but calling {@link Flow.Subscriber#onNext} is optional. As an
   * alternative the {@link Flow.Publisher} can call {@link Operation#submit}. Since
   * {@link Flow.Subscriber#onNext} submits the {@link Operation} only one of the two can be
   * called otherwise the {@link Operation} is submitted twice. Calling
   * {@link Operation#submit} decrements the request count so far as the
   * {@link Flow.Subscriber} is concerned.
   *
   * ISSUE: This is a hack. The {@code submit} or {@code onNext} alternative is
   * weird but necessary. Other choices include calling only {@code submit} or
   * requiring the {@link Flow.Publisher} to call both, {@code onNext} first then
   * {@code submit}. Neither of those seems better. Calling only {@code onNext}
   * isn't acceptable as then there is no way to get access to the
   * {@link Submission} or the {@link CompletableFuture}.
   *
   * @param publisher
   * @return this OperationGroup
   */
  public OperationGroup<S, T> operationPublisher(Flow.Publisher<Operation> publisher);

  /**
   * Supply a {@link Logger} for the implementation of this
   * {@link OperationGroup} to use to log significant events. Exactly what
   * events are logged, at what Level the events are logged and with what
   * parameters is implementation dependent. All member {@link Operation}s of
   * this {@link OperationGroup} will use the same {@link Logger} except a
   * member {@link OperationGroup} that is supplied with a different
   * {@link Logger} uses that {@link Logger}.
   *
   * Supplying a {@link Logger} configured with a
   * {@link java.util.logging.MemoryHandler} with the
   * {@link java.util.logging.MemoryHandler#pushLevel} set to
   * {@link java.util.logging.Level#WARNING} will result in no log output in
   * normal operation. In the event of an error the actions leading up to the
   * error will be logged.
   *
   * Implementation Note: Implementations are encouraged to log the creation of
   * this {@link OperationGroup} set to {@link java.util.logging.Level#INFO}, the
   * creation of member {@link Operation}s at the
   * {@link java.util.logging.Level#CONFIG} level, and execution of member
   * {@link Operation}s at the {@link java.util.logging.Level#FINE} level.
   * Detailed information about the execution of member {@link Operation}s may
   * be logged at the {@link java.util.logging.Level#FINER} and
   * {@link java.util.logging.Level#FINEST} levels. Errors in the execution of
   * user code should be logged at the {@link java.util.logging.Level#WARNING}
   * Level. Errors in the implementation code should be logged at the
   * {@link java.util.logging.Level#SEVERE} Level.
   *
   * @param logger used by the implementation to log significant events
   * @return this {@link OperationGroup}
   */
  public OperationGroup<S, T> logger(Logger logger);

  @Override
  public OperationGroup<S, T> timeout(long milliseconds);
}
