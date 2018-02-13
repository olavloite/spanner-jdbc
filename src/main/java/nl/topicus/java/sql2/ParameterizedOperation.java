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
import java.util.concurrent.CompletionStage;

/**
 * An Operation that has in parameters.
 *
 * As the SQL is vendor specific, how parameters are represented in the SQL is
 * itself vendor specific.
 *
 * For positional parameters, those where all parameters are indicated by the
 * same character sequence, for example '?', it is recommended that the
 * parameter id be the decimal integer representation of the parameter number.
 * 
 * A SQL structured type passed as an argument to a set method must be created
 * by the same {@link Connection} that the created the
 * {@link ParameterizedOperation}. If not {@link IllegalArgumentException} is
 * thrown. A SQL structured type is one of
 * {@link SqlArray}, {@link SqlBlob}, {@link SqlClob}, {@link SqlRef} or
 * {@link SqlStruct}. This limitation holds recursively for all components of
 * a SQL structured type. An implementation may relax this constraint.
 *
 * @param <T> the type of the result of this {@link Operation}
 */
public interface ParameterizedOperation<T> extends Operation<T> {

  /**
   * Set a parameter value. The value is captured and should not be modified
   * before the {@link Operation} is completed.
   *
   * @param id the identifier of the parameter marker to be set
   * @param value the value the parameter is to be set to
   * @param type the SQL type of the value to send to the database
   * @return this Operation
   */
  public ParameterizedOperation<T> set(String id, Object value, SqlType type);

  /**
   * Set a parameter value. Use a default SQL type determined by the type of the
   * value argument. The value is captured and should not be modified before the
   * {@link Operation} is completed.
   *
   * @param id the identifier of the parameter marker to be set
   * @param value the value the parameter is to be set to
   * @return this {@link Operation}
   */
  public ParameterizedOperation<T> set(String id, Object value);

  /**
   * Set a parameter value to be the future value of a
   * {@link java.util.concurrent.CompletableFuture}. The {@link Operation} will
   * not be executed until the {@link java.util.concurrent.CompletableFuture} is
   * completed. This method allows submitting {@link Operation}s that depend on
   * the result of previous {@link Operation}s rather than requiring that the
   * dependent {@link Operation} be submitted only when the previous
   * {@link Operation} completes.
   *
   * ISSUE: Should the second arg be a {@link java.util.concurrent.Future}? The
   * advantage is that there are Futures that are not
   * {@link java.util.concurrent.CompletableFuture}s. The disadvantage is that
   * {@link java.util.concurrent.Future}s have to be polled whereas
   * {@link java.util.concurrent.CompletableFuture} has callbacks. This is
   * really only an issue for the implementation. Semantically it makes no
   * difference.
   *
   * @param id the identifier of the parameter marker to be set
   * @param valueFuture the {@link java.util.concurrent.Future} that provides
   * the value the parameter is to be set to
   * @param type the SQL type of the value to send to the database
   * @return this {@link Operation}
   */
  public ParameterizedOperation<T> set(String id, CompletableFuture<?> valueFuture, SqlType type);

  /**
   * Set a parameter value to be the future value of a
   * {@link java.util.concurrent.CompletableFuture}. The {@link Operation} will
   * not be executed until the {@link java.util.concurrent.CompletableFuture} is
   * completed. This method allows submitting {@link Operation}s that depend on
   * the result of previous {@link Operation}s rather than requiring that the
   * dependent {@link Operation} be submitted only when the previous
   * {@link Operation} completes. Use a default SQL type determined by the type
   * of the value of the {@link java.util.concurrent.CompletableFuture}
   * argument. The value of the {@link java.util.concurrent.CompletableFuture}
   * is captured and should not be modified before the {@link Operation} is
   * completed.
   *
   * ISSUE: Should the second arg be a {@link java.util.concurrent.Future}? The
   * advantage is that there are {@link java.util.concurrent.Future}s that are
   * not {@link java.util.concurrent.CompletableFuture}s. The disadvantage is
   * that {@link java.util.concurrent.Future}s have to be polled whereas
   * {@link java.util.concurrent.CompletableFuture} has callbacks. This is
   * really only an issue for the implementation. Semantically it makes no
   * difference.
   *
   * @param id the identifier of the parameter marker to be set
   * @param valueFuture the {@link java.util.concurrent.CompletableFuture} that
   * provides the value the parameter is to be set to
   * @return this {@link Operation}
   */
  public ParameterizedOperation<T> set(String id, CompletableFuture<?> valueFuture);

  /**
   * Convenience method. Since many
   * {@link java.util.concurrent.CompletableFuture} methods return
   * {@link java.util.concurrent.CompletionStage}, this method (slightly) simplifies the code by
   * encapsulating the call to {@link java.util.concurrent.CompletionStage#toCompletableFuture}. The value of the
   * {@link java.util.concurrent.CompletionStage} is captured and should not be modified before the
   * {@link Operation} is completed.
   *
   * @param id the identifier of the parameter marker to be set
   * @param value a {@link java.util.concurrent.CompletionStage} the result of which is the value the
   * parameter is to be set to
   * @return this {@link Operation}
   */
  default public ParameterizedOperation<T> set(String id, CompletionStage value) {
    return set(id, value.toCompletableFuture());
  }

  /**
   * Convenience method. Since many
   * {@link java.util.concurrent.CompletableFuture} methods return
   * {@link java.util.concurrent.CompletionStage}, this method (slightly) simplifies the code by
   * encapsulating the call to {@link java.util.concurrent.CompletionStage#toCompletableFuture}. The value of the
   * {@link java.util.concurrent.CompletionStage} is captured and should not be modified before the
   * {@link Operation} is completed.
   *
   * @param id the identifier of the parameter marker to be set
   * @param value a {@link java.util.concurrent.CompletionStage} the result of which is the value the
   * parameter is to be set to
   * @param type the SQL type of the value to be sent to the database
   * @return this {@link Operation}
   */
  default public ParameterizedOperation<T> set(String id, CompletionStage value, SqlType type) {
    return set(id, value.toCompletableFuture(), type);
  }

}
