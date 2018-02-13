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


/**
 * <p>
 * An API for accessing and processing data stored in a data source (usually a
 * relational database) using the Java&trade; programming language. This API
 * includes a framework whereby different drivers can be installed dynamically
 * to access different data sources. This API is specifically geared for passing
 * SQL statements to a database though it may be used for reading and writing
 * data from/to any data source that has a tabular format.</p>
 *
 * <p>
 * This API differs from the API in {@link java.sql} in several ways.</p>
 * <ul>
 * <li>Is asynchronous
 * <li>Is geared toward high-throughput programs
 * <li>Does not attempt to support every database feature
 * <li>Does not attempt to abstract the database
 * <li>Uses the builder pattern
 * <li>Supports the fluent programming style
 * </ul>
 *
 * <p>
 * It is worth emphasizing that this API is an alternate to the {@link java.sql}
 * API, not a replacement. There are many programs that can much more readily be
 * written using the {@link java.sql} API as it has many features that are not
 * available in this API. For example this API provides almost no mechanism for
 * getting metadata.</p>
 *
 * <p>
 * This API is not an extension to the {@link java.sql} API. It an independent
 * API and is used on its own without reference to java.sql. With one exception
 * this API makes no reference to {@link java.sql} types. The one exception is
 * that {@link nl.topicus.java.sql2.SqlException} extends {@link java.sql.SQLException}.
 * This permits outer layers to catch database errors thrown by either API
 * without change.</p>
 *
 *
 * <h3>Overview</h3>
 *
 * <p>
 * The core feature of this API is that it is asynchronous. With very few
 * exceptions, no method call will wait for a network operation. Those very few
 * methods that do wait are clearly indicated by including the word
 * "{@code Wait}" in their names.</p>
 *
 * <p>
 * Possibly blocking actions are represented as {@link Operation}s. An
 * application using the API creates and submits one or more {@link Operation}s.
 * The driver executes these {@link Operation}s asynchronously, reporting their
 * results via {@link java.util.concurrent.CompletableFuture}s. An application
 * can respond to the results via the
 * {@link java.util.concurrent.CompletableFuture}s or via callbacks that can be
 * configured on many of the {@link Operation}s or both. Creating and submitting
 * {@link Operation}s is strictly non-blocking. Handling the results of possibly
 * blocking {@link Operation}s is done asynchronously. No application thread
 * will ever block unless it intentionally calls one of the few clearly
 * identified methods that may block. Calling a blocking method is entirely
 * optional.</p>
 *
 * <p>
 * All {@link Operation}s provide a
 * {@link java.util.concurrent.CompletableFuture}. The value of that
 * {@link java.util.concurrent.CompletableFuture} is the value of the
 * {@link Operation}, set when the {@link Operation} completes. Some
 * {@link Operation}s provide callbacks for processing the result of the
 * {@link Operation} independent of the
 * {@link java.util.concurrent.CompletableFuture}. Those {@link Operation}s can
 * be used for executing SQL that returns results of a specific type. For
 * example SQL that returns a row sequence would be executed with a
 * {@link RowOperation}. A {@link RowOperation} provides callbacks for
 * processing each row and for aggregating the results of processing the rows.
 * Other {@link Operation}s are specialized for SQL that returns a count or that
 * returns out parameters. The choice of {@link Operation} is dependent on the
 * result to be processed and is independent of the particular kind of SQL
 * statement.</p>
 *
 * <p>
 * An {@link OperationGroup} encapsulates a group of {@link Operation}s and
 * executes them using common attributes. An {@link OperationGroup} can be
 * unconditional or conditional, sequential or parallel, dependent or
 * independent, or any combination of these. Dependent/independent controls
 * error handling. If one member of a dependent {@link OperationGroup} fails the
 * remaining not-yet-executed members are completed exceptionally. If the
 * {@link OperationGroup} is independent, the member {@link Operation}s are
 * executed regardless of whether one or more fails.</p>
 *
 * <p>
 * A {@link Connection} is itself an {@link OperationGroup} and so can be
 * conditional, parallel, or independent, but by default is unconditional,
 * sequential, dependent. While a {@link Connection} may be created with values
 * other than the defaults, using the defaults is by far the most common case.
 * The API provides convenience methods that support this case. Using these
 * convenience methods is recommended in all but the most unusual circumstances.
 * In particular making the {@link Connection} parallel introduces some
 * challenges that would require a full understanding of the details of the API.
 * It would almost certainly be better to create a parallel
 * {@link OperationGroup} within the {@link Connection}.</p>
 *
 * <p>
 * <i>
 * ISSUE: Should we disallow {@code Connection.parallel()}?</i></p>
 *
 * <p>
 * The {@link java.sql} API frequently provides many ways to do the same thing.
 * This API makes no attempt to do this. For those capabilities this API
 * supports, it frequently defines exactly one way to do something. Doing things
 * another way, for example calling methods in a non-standard order, frequently
 * results in an IllegalStateException. This approach is intended to make things
 * simpler for both the user and the implementor. Rather than having to
 * understand complicated interactions of many different components and methods
 * executed in any order, the intent is that there is only one way to do things
 * so only one path must be understood or implemented. Anything off that path is
 * an error. While this requires a programmer to write code in one specific way
 * it makes things easier on future maintainers of the code as the code will
 * conform to the standard pattern. Similarly the implementation is simplified
 * as only the standard use pattern is supported.</p>
 *
 * <p>
 * One way this API simplifies things in to define types as single use. Many
 * types are created, configured, used once, and are then no longer usable. Many
 * configuration methods can be called only once on a given instance. Once an
 * instance is configured it cannot be reconfigured. Once an instance is used it
 * cannot be reused. This simplifies things by eliminating the need to
 * understand and implement arbitrary sequences of method calls that reconfigure
 * and reuse instances. Since objects are single use there is no expectation
 * that an application cache or share {@link Operation}s.</p>
 *
 * <p>
 * While the user visible types are single use, it is expected that an
 * implementation will cache and reuse data and {@link Object}s that are worth
 * the effort. Rather than attempt to guess what an implementation should reuse
 * and capture that in the API, this API leaves it entirely up to the
 * implementation. Since the API specifies very little reuse, an implementation
 * is free to reuse whatever is appropriate. Since the pattern of use is
 * strictly enforced figuring out how to reuse objects is greatly
 * simplified.</p>
 *
 * <p>
 * The {@link java.sql} API provides many tools for abstracting the database,
 * for enabling the user to write database independent code. This API does not.
 * It is not a goal of this API to enable users to write database independent
 * code. That is not to say it is not possible, just that this API does not
 * provide tools to support such. Abstraction features typically impose
 * performance penalties on some implementations. As this API is geared for
 * high-throughput programs it avoids such abstractions rather than reduce
 * performance.</p>
 *
 * <p>
 * One such abstraction feature is the JDBC escape sequences. Implementing these
 * features requires parsing the SQL so as to identify the escape sequences and
 * then generating a new String with the vendor specific SQL corresponding to
 * the escape sequence. This is an expensive operation. Further each SQL must be
 * parsed whether it contains an escape sequence or not imposing the cost on all
 * JDBC users, not just the ones who use escape sequences. The same is true of
 * JDBC parameter markers. The SQL accepted by this API is entirely vendor
 * specific, including parameter markers. There is no need for pre-processing
 * prior to SQL execution substantially reducing the amount of work the
 * implementation must do.</p>
 *
 * <p>
 * Note: It would be a reasonable future project to develop a SQL builder API
 * that creates vendor specific SQL from some more abstract representation.</p>
 *
 * <h3>Execution Model</h3>
 *
 * <p>
 * <i>This section describes the function of a conforming implementation. It is
 * not necessary for an implementation to be implemented as described only that
 * the behavior be the same.</i></p>
 *
 * <p>
 * An {@link Operation} has an action and a
 * {@link java.util.concurrent.CompletableFuture}. Some {@link Operation}s have
 * some form of result processor.</p>
 *
 * <p>
 * An {@link Operation} is executed by causing the action to be performed,
 * processing the result of the action if there is a result processor, and
 * completing the {@link java.util.concurrent.CompletableFuture} with the result
 * of the result processor if there is one or with the result of the action if
 * there is no result processor. If the action or the result processing causes
 * an unhandled error the {@link java.util.concurrent.CompletableFuture} is
 * completed exceptionally.</p>
 *
 * <p>
 * Performing the action may require one or more interactions with the database.
 * These interactions may be carried out in parallel with processing the result.
 * If the database result is ordered, that result is processed in that order.
 *
 * <p>
 * An {@link OperationGroup} has a collection of {@link Operation}s and
 * optionally a condition. For a sequential {@link OperationGroup}
 * {@link Operation}s are selected from the collection in the order they were
 * submitted. For a parallel {@link OperationGroup} {@link Operation}s are
 * selected from the collection in any order.</p>
 *
 * <p>
 * The action of an {@link OperationGroup} is performed as follows:
 * <ul>
 * <li>
 * If the {@link OperationGroup} has a condition, the value of the condition is
 * retrieved. If the value is {@link Boolean#FALSE} the action is complete and
 * the {@link java.util.concurrent.CompletableFuture} is completed with null. If
 * the value completes exceptionally the action is complete and the
 * {@link java.util.concurrent.CompletableFuture} is completed exceptionally
 * with the same exception. If the condition value is {@link Boolean#TRUE} or
 * there is no condition the {@link Operation}s in the collection are executed
 * and their results processed. The action is complete when the
 * {@link OperationGroup} is not held and all the {@link Operation}s have been
 * executed.</li>
 * <li>
 * If the {@link OperationGroup} is parallel more than one {@link Operation} may
 * be executed at a time.</li>
 * <li>
 * If the {@link OperationGroup} is dependent and an {@link Operation} completes
 * exceptionally all {@link Operation}s in the collection that are yet to begin
 * execution are completed exceptionally with a {@link SqlSkippedException}. The
 * cause of that exception is the {@link Throwable} that caused the
 * {@link Operation} to be completed exceptionally. If an {@link Operation} is
 * in flight when another {@link Operation} completes exceptionally the in
 * flight {@link Operation} may either be allowed to complete uninterrupted or
 * it may be completed exceptionally. The {@link OperationGroup} is completed
 * exceptionally with the {@link Throwable} that caused the {@link Operation} to
 * complete exceptionally. Note: the {@link Operation} returned by
 * {@link Connection#closeOperation} is never skipped, never completed
 * exceptionally with {@link SqlSkippedException}. It is always executed.</li>
 * <li>
 * If the {@link OperationGroup} is independent and an {@link Operation}
 * completes exceptionally all other {@link Operation}s are executed regardless.
 * There is no result to be processed for an {@link Operation} that completed
 * exceptionally. The {@link OperationGroup} is not completed exceptionally as
 * the result of one or more {@link Operation}s completing exceptionally.</li>
 * </ul>
 *
 * <p>
 * A {@link Connection} is a distinguished {@link OperationGroup}. A
 * {@link Connection} is executed upon being submitted.</p>
 *
 * <h3>Transactions</h3>
 *
 * <p>
 * <i>This section describes the function of a conforming implementation. It is
 * not necessary for an implementation to be implemented as described only that
 * the behavior be the same.</i></p>
 *
 * <p>
 * An implementation has only limited control over transactions. SQL statements
 * can start, commit, and rollback transactions without the implementation
 * having any influence or even being aware. This specification only describes
 * the behavior of those transaction actions that are visible to and controlled
 * by the implementation, i.e. commit and rollback {@link Operation}s.
 * Transaction actions caused by SQL may interact with actions controlled by the
 * implementation in unexpected ways.</p>
 *
 * <p>
 * The creation of Operations and the subsequent execution of those Operations
 * are separated in time. It is quite reasonable to specify that a transaction
 * should commit after the Operations that perform that transaction are created.
 * But if the execution of the transaction does not result in the expected results
 * it might be necessary to rollback the transaction rather than commit it. This
 * determination depends on the execution of the Operations long after the
 * commit Operation is created. To address this mismatch, the commit Operation
 * specified by this API is conditional. Although it is called a "commit
 * Operation" it commits the transaction only by default. It can be caused to
 * rollback the transaction or even to do nothing at any time before it is
 * executed.</p>
 *
 * <p>
 * A commit Operation, like all Operations, is immutable once submitted. But a
 * commit Operation depends upon a Transaction and a Transaction can be set to
 * commit or rollback. A Transaction controls the next commit
 * Operation submitted after the Transaction is created. Using this mechanism an
 * error handler, result handler or other code can cause a subsequent commit
 * Operation to rollback instead of commit.</p>
 *
 * <pre>
 * {@code
 *   Transaction t = conn.getTransaction();
 *   conn.countOperation(updateSql)
 *       .resultProcessor( count -> { 
 *           if (count > 1) t.setRollbackOnly(); 
 *           return null; 
 *       } )
 *       .submit();
 *   conn.commit();
 * }
 * </pre>
 *
 * <p>
 * In this example if the update SQL modifies more than one row the result
 * processor will set the Transaction to rollback only. When the subsequent
 * commit Operation is executed it will (somewhat in spite of its name) cause
 * the transaction to rollback.</p>
 *
 * <h4>ISSUE</h4>
 *
 * <p>
 * This API creates an opaque relationship between a Transaction and the
 * dependent commit Operation. An alternate mechanism would put the
 * getTransaction method on a new type, CommitOperation which would yield code
 * like this:</p>
 *
 * <pre>
 * {@code
 *   CommitOperation commitOp = conn.commitOperation();
 *   Transaction t = commitOp.getTransaction();
 *   conn.countOperation(updateSql)
 *       .resultProcessor( count -> { if (count > 1) t.setRollbackOnly(); return null; } )
 *       .submit();
 *   commitOp.submit();
 * }
 * </pre>
 *
 * <p>
 * This approach makes the relationship between the commit Operation and the
 * Transaction apparent but requires creating a new type CommitOperation and
 * makes the programmer follow a different pattern of creating an Operation, the
 * CommitOperation, long before it is submitted. Moving the setRollbackOnly and
 * other methods from Transaction to CommitOperation eliminates the need for the
 * Transaction type but makes CommitOperation mutable which while functionally
 * identical is a violation of one of the design goals.</p>
 *
 * <p>
 * The specified mechanism where the action of a commit() method call depends on
 * a Transaction object to which it is somewhat indirectly related is lifted
 * from JPA which has this same pattern. Because this mechanism survived the
 * scrutiny of the JPA EG and reviewers it seems a reasonable choice here.</p>
 *
 * <h3>POJOs</h3>
 *
 * <p>
 * This API supports setting and getting POJOs (Plain Old Java Objects) as
 * parameter values and results. This is not a comprehensive ORM (Object
 * Relational Mapping). It is just a convenience.
 *
 * <p>
 * To set parameters of a SQL statement to the values of a POJO, the type of the
 * POJO must be annotated with the @SqlParameter annotation. One or more public
 * getter methods must be so annotated. The @SqlParameter annotation defines the
 * parameter marker base name and SQL type. The call to the
 * ParameterizedOperation.set method provides the POJO itself and the parameter
 * marker prefix. The implementation binds the return value of the annotated
 * method to the parameter identified by the parameter marker prefix prepended
 * to the parameter marker base name.</p>
 *
 * <pre>
 * {@code
 *   public class Employee {
 *     private String name;
 *     private String department;
 *     \@SqlParameter("name", "VARCHAR")
 *     public String getName() { return name; }
 *     public String getDepartement() { return department; }
 *     \@SqlColumns("name", "dept")
 *     public static Employee createEmployee(String name, String department) {
 *       Employee e = new Employee();
 *       e.name = name;
 *       e.department = department;
 *       return e;
 *     }
 *   }
 *
 *   conn.countOperation("insert into emp values(:emp_name, :emp_dept)")
 *       .set("emp_", employee);
 * }
 * </pre>
 *
 * <p>
 * The above code fragment is identical to</p>
 *
 * <pre>
 * {@code
 *    conn.countOperation("insert into emp values(emp_name, emp_dept)")
 *       .set("emp_name", employee.getName(), JdbcType.VARCHAR);
 *       .set("emp_dept", employee.getDepartment(), JdbcType.VARCHAR)
 * }
 * </pre>
 *
 * <p>
 * The prefix may be the empty string but may not be null. IllegalStateException
 * is thrown if a parameter is set more than once either by a POJO set or a
 * single value set. Implementations may attempt to bind a parameter for every
 * annotated getter method. An implementation is not required to check whether
 * or not there is a parameter with the specified name. It is implementation
 * dependent whether an annotated getter with no corresponding parameter is an
 * error or not and whether the implementation or the database detect it if it
 * is an error.</p>
 *
 * <p>
 * To construct a POJO from the result of an Operation the type of the POJO must
 * be annotated with @SqlColumns. The annotation may be applied to a public
 * static factory method, a public constructor, or one or more public setter
 * methods. If applied to setters, there must be a public zero-arg constructor
 * or public static zero-arg factory method. The annotation provides the base
 * names for column or out parameter marker that provides the value for the
 * corresponding parameter or setter method. The get method call provides the
 * prefix.</p>
 *
 * <pre>
 * {@code
 *     conn.rowOperation("select name, dept from emp")
 *     .initialValue( () -> new ArrayList() )
 *     .rowAggregator( (l, r) -> { 
 *         l.add(r.get("", Employee.class)); 
 *         return l; 
 *     } )
 * }
 * </pre>
 *
 * <p>
 * The above code fragment is identical to</p>
 *
 * <pre>
 * {@code
 *     conn.rowOperation("select name, dept from emp")
 *       .initialValue( () -> new ArrayList() )
 *       .rowAggregator( (l, r) -> {
 *           l.add(Employee.createEmployee(r.get("name", String.class), 
 *                                         r.get("dept", String.class))); 
 *           return l; 
 *       } )
 * }
 * </pre>
 *
 * <p>
 * If more than one factory method, constructor, or set of setters is annotated
 * it is implementation dependent which is used to construct a POJO. An
 * implementation is not required to determine the best choice for any meaning
 * of "best". An implementation my instead throw an exception if more than one
 * alternative is annotated. If setters are annotated an implementation should
 * call every annotated setter. It is implementation dependent whether it
 * attempts to call a subset of the setters if all the columns named in the
 * annotations are not present. In summary, best practice is to annotate exactly
 * what is required and no more and to use exactly what is annotated and no more.
 * </p>
 *
 */
package nl.topicus.java.sql2;

