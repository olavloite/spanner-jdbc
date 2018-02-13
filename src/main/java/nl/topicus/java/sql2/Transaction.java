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

/**
 * Represents the database transaction open when the next submitted commit or
 * rollback {@link Operation} is executed.
 *
 * As this API is asynchronous, the transaction active in the database at the
 * moment an {@link Operation} is created or submitted is likely to be unrelated
 * to that {@link Operation}. This type is an abstraction of the (possibly)
 * future database transaction that a currently created and submitted
 * {@link Operation} belongs to. This enables an error handler or result
 * processor to influence the currently active transaction. If such a processor
 * were to submit a commit or rollback {@link Operation} the active transaction
 * when the commit or rollback is executed might be unrelated to the
 * {@link Operation} that caused it to be submitted.
 *
 * While there might be exceptions, in general an error handler or result
 * processor should never submit a commit or rollback {@link Operation}. Instead
 * a commit {@link Operation} should be submitted in sequence with the
 * {@link Operation} containing the error handler. The error handler should have
 * access to a {@link Transaction} retrieved when its {@link Operation} is
 * submitted. If the error handler should cause a rollback instead of a commit
 * the error handler should call {@link setRollbackOnly} on that
 * {@link Transaction}.
 *
 * Example:
 *
 * <pre>
 * {@code
 *   Transaction t = conn.getTransaction();
 *   conn.countOperation(updateSql)
 *       .resultProcessor( count -> { if (count > 1) t.setRollbackOnly(); } )
 *       .submit();
 *   conn.commit();
 * }</pre>
 *
 * When the result processor is executed it is quite likely that a close
 * {@link Operation} has already been submitted. If instead of executing
 * {@code t.setRollbackOnly()} the result processor instead submitted a rollback
 * {@link Operation} that would likely fail since the {@link Connection} quite
 * likely already had a queued close {@link Operation}.
 *
 * Similarly if the result processor executed
 * {@code conn.getTransaction().setRollbackOnly()} that would quite likely fail
 * as database transaction referenced by the returned {@link Transaction} is the
 * transaction active when the next next commit or rollback {@link Operation} is
 * submitted. Since the {@link Connection} might already be closed there would
 * be no subsequent commit or rollback {@link Operation}.
 *
 * NOTE: The {@link Connection#commit} and {@link Connection#rollback} methods
 * are on {@link Connection}, not {@link Transaction}. These methods queue an
 * {@link Operation} to end the transaction current at the time these
 * {@link Operation}s are executed. As such they are actions on the
 * {@link Connection} not the {@link Transaction}.
 */
public interface Transaction {

  /**
   * Force this {@link Transaction} to rollback when it ends. The
   * {@link Transaction} ends when the commit {@link Operation} (or rollback
   * {@link Operation}) for this {@link Transaction} is executed. If this
   * {@link Transaction} has been {@link setRollbackOnly} then the commit
   * {@link Operation} will end this {@link Transaction} with a rollback. The
   * rollback {@link Operation} always ends the {@link Transaction} with a
   * rollback.
   *
   * @return This {@link Transaction}
   * @throws IllegalStateException if {@link isActive()} is false.
   */
  public Transaction setRollbackOnly();

  /**
   * Returns {@code true} iff the {@link setRollbackOnly} method has been called
   * on this Transaction
   *
   * @return {@code true} if {@link setRollbackOnly} has been called.
   * @throws IllegalStateException if {@link isActive()} is false.
   */
  public boolean isRollbackOnly();

  /**
   * Returns {@code true} iff this {@link Transaction} has not ended, ie if a
   * commit {@link Operation} or rollback {@link Operation} for this
   * {@link Transaction} has not been executed.
   *
   * @return true iff this {@link Transaction} has not ended.
   */
  public boolean isActive();

}
