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
 * A sequence of {@link CountOperation}s that can be executed in one database
 * operation. Exactly how this happens is vendor specific. The member
 * {@link CountOperation}s are completed exceptionally before the
 * {@link BatchCountOperation} is completed exceptionally.
 *
 * @param <T> the type of the result of aggregating results the member
 * Operations
 */
public interface BatchCountOperation<T> extends Operation<T> {

  /**
   * Returns a new member {@link CountOperation} that will be executed as part of
   * this {@link BatchCountOperation}. These {@link CountOperation}s will be
   * executed in the order submitted, not in the order created. If a member
   * {@link CountOperation} is not submitted when this
   * {@link BatchCountOperation} is submitted, subsequently submitting that
   * member CountOperation throws {@link IllegalStateException}.
   *
   * @return a new CountOperation
   * @throws IllegalStateException if this {@link BatchCountOperation} has been
   * submitted
   */
  public ParameterizedCountOperation countOperation();

  /**
   * A supplier for the value passed to the {@link countAggregator} for the
   * first {@link CountOperation} result processed.
   *
   * The default initial value is {@code () -&lt; null}.
   *
   * @param supplier supplies the initial value for the member result aggregator
   * @return this {@link BatchCountOperation}
   * @throws IllegalStateException if the {@link Operation} has been submitted
   * or this method has been called previously.
   */
  public BatchCountOperation<T> initialValue(Supplier<? extends T> supplier);

  /**
   * A binary function that is applied to the result of each
   * {@link CountOperation} executed by this {@link BatchCountOperation}.
   *
   * The default value is {@code (t,c) -&lt; null}.
   *
   * @param aggregator a binary function that aggregates the member results
   * @return this {@link BatchCountOperation}
   * @throws IllegalStateException if the {@link Operation} has been submitted
   * or this method has been called previously.
   */
  public BatchCountOperation<T> countAggregator(BiFunction<? super T, Result.Count, ? extends T> aggregator);

  /**
   * Add this {@link BatchCountOperation} to the {@link Connection}'s
   * collection. All of the member {@link CountOperation}s previously submitted
   * will be executed when this {@link BatchCountOperation} is executed.
   *
   * @return a {@link Submission} for this {@link BatchCountOperation}
   * @throws IllegalStateException if the {@link Operation} has been submitted.
   */
  @Override
  public Submission<T> submit();

}
