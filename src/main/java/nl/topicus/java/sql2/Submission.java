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
import java.util.concurrent.Future;

/**
 * The result of submitting an {@link Operation}. The {@link cancel} method of a
 * {@link CompletableFuture} does not cancel the {@link Operation}. This is part
 * of the contract of {@link CompletableFuture}. This type provides a method to
 * cancel the {@link Operation}. Canceling an {@link Operation} only makes sense
 * after the {@link Operation} is submitted so this type is the result of
 * submitting an {@link Operation}.
 *
 * @param <T> The type of the result of the {@link Operation} that created this
 * {@link Submission}
 */
public interface Submission<T> {

  /**
   * Request that the {@link Operation} not be executed or that its execution be
   * aborted if already begun. This is a best effort action and may not succeed
   * in preventing or aborting the execution. This method does not block.
   * 
   * If execution is prevented the Operation is completed exceptionally with
   * SkippedSqlException. If the Operation is aborted it is completed
   * exceptionally with SqlException.
   *
   * @return a {@link java.util.concurrent.Future} that is true if the {@link Operation} is canceled.
   */
  public Future<Boolean> cancel();

  /**
   * Returns a {@link CompletableFuture} which value is the result of the
   * {@link Operation}. Any actions on the returned {@link CompletableFuture},
   * eg completeExceptionally or cancel, have no impact on this
   * {@link Operation}. If this {@link Operation} is already completed the
   * returned {@link CompletableFuture} will be completed.
   *
   * Each call of this method for a given {@link Operation} returns the same
   * {@link CompletableFuture}.
   *
   * @return a {@link java.util.concurrent.CompletableFuture} for the result of this
   * {@link Operation}. Retained.
   */
  public CompletableFuture<T> toCompletableFuture();

}
