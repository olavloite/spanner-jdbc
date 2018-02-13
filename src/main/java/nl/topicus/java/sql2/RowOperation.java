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

import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * A {@link RowOperation} is a database operation that returns a row sequence.
 * 
 * @param <T> the type of the result of this {@link Operation}
 */
public interface RowOperation<T> extends Operation<T> {

  /**
   * A hint to the implementation of how many rows to fetch in each database
   * access. Implementations are free to ignore it.
   * 
   * @param rows suggested number of rows to fetch per access
   * @return this {@link RowOperation}
   * @throws IllegalArgumentException if row &lt; 1
   * @throws IllegalStateException if this method had been called previously
   */
  public RowOperation<T> fetchSize(long rows) throws IllegalArgumentException;
  
  /**
   * A {@link Supplier} for the initial value passed to the {@link rowAggregator}. The default 
   * value is {@code () -&lt; null}.
   *
   * @param supplier a {@link Supplier} for the initial value for the 
   * {@link rowAggregator}. Not {@code null}.
   * @return this {@link RowOperation}
   * @throws IllegalStateException if this method had been called previously
   */
  public RowOperation<T> initialValue(Supplier<? extends T> supplier);

  /**
   * A function to handle the {@link Result.Row}s of the row sequence. This function is called
   * once for each {@link Result.Row}. The arguments to the first call are the result of the
   * {@link initialValue} {@link Supplier} and the first {@link Result.Row}. The arguments to subsequent calls are
   * the result of the previous call and the current {@link Result.Row}. The result of the last call
   * is the result of this {@link Operation}. The default value is {@code (t,r) -&lt; null}.
   *
   * @param aggregator a {@link BiFunction} that produces the aggregate value over all
   * the rows
   * @return this {@link RowOperation}
   * @throws IllegalStateException if this method had been called previously
   */
  public RowOperation<T> rowAggregator(BiFunction<? super T, Result.Row, ? extends T> aggregator);

}
