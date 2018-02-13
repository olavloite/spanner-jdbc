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
import java.util.function.Function;

/**
 *
 * A multi-operation is an {@link Operation} that returns one or more results in
 * addition to the result defined by the {@link Operation}. A {@link DynamicMultiOperation} is a
 * multi-operation where the number and types of the results are determined at
 * execution.
 * 
 * NOTE: In general one way to do things is sufficient however the API provides
 * two ways to handle multiple results. This way, the dynamic way, is required
 * because the number and type of results cannot always be known in advance. The
 * static way is also provided because it is much easier to use when the number
 * and type of results is known. The improvement in ease of use outweighs the
 * duplication IMO. If necessary one or the other can be eliminated. Eliminating
 * dynamic reduces functionality. Eliminating static reduces ease of use in what
 * I believe to be a common case.
 *
 * @param <T> type of the result of this DynamicMultiOperation
 */
public interface DynamicMultiOperation<T> extends OutOperation<T> {

  /**
   * A handler for a {@link DynamicMultiOperation}.
   * 
   * @param <T> the type of the return value of this ResultHandler
   */
  interface ResultHandler<T> {

    /**
     * Handle the next result of a multi-operation. Exactly one of of the
     * parameters will be not {@code null}. The other will be {@code null}. If the not-null
     * parameter value is not submitted prior to the call to this method returning,
     * it will be submitted when the call returns.
     *
     * @param resultNumber The number of times this handler has been called. The
     * first call is 1.
     * @param countOp not {@code null} iff the next result is a count
     * @param rowOp not null iff the next {@code result} is a row sequence
     */
    public void handle(int resultNumber, CountOperation<T> countOp, RowOperation<T> rowOp);
  }

  /**
   * Specifies a method that will handle the next result. The method will be
   * called once for each result. Exactly one of of the parameters will be not
   * {@code null}. The other will be {@code null}. If the not-{@code null} parameter value is not
   * submitted prior to the call to handle returning, it will be submitted when
   * the call returns. The default is {@code (n, c, r) -> null}.
   *
   * @param handler a result handler for processing the {@link Result}s of this
   * {@link Operation}. Not {@code null}.
   * @return this {@link DynamicMultiOperation}
   */
  public DynamicMultiOperation<T> resultHandler(ResultHandler<T> handler);

  // Covariant overrides
  
  @Override
  public DynamicMultiOperation<T> outParameter(String id, SqlType type);
  
  @Override
  public DynamicMultiOperation<T> resultProcessor(Function<Result.OutParameterMap, ? extends T> processor);

  @Override
  public DynamicMultiOperation<T> set(String id, Object value);

  @Override
  public DynamicMultiOperation<T> set(String id, Object value, SqlType type);

  @Override
  public DynamicMultiOperation<T> set(String id, CompletableFuture<?> valueFuture);

  @Override
  public DynamicMultiOperation<T> set(String id, CompletableFuture<?> valueFuture, SqlType type);

  @Override
  public default DynamicMultiOperation<T> set(String id, CompletionStage value) {
    return set(id, value.toCompletableFuture());
  }

  @Override
  public default DynamicMultiOperation<T> set(String id, CompletionStage value, SqlType type) {
    return set(id, value.toCompletableFuture(), type);
  }

  @Override
  public DynamicMultiOperation<T> timeout(long milliseconds);

}
