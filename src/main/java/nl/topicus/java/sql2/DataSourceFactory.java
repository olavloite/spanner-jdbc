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
 * This interface supports injecting a {@link DataSourceFactory}. The SPI
 * mechanism will register {@link DataSourceFactory} implementations with the
 * given name.
 *
 */
public interface DataSourceFactory {

  /**
   * Uses SPI to find a {@link DataSourceFactory} with the requested name or
   * {@code null} if one is not found. By convention {@link DataSourceFactory}
   * names follow the same rules as Java package names.
   *
   * @param name the name that identifies the factory
   * @return a {@link DataSourceFactory} for {@code name} or {@code null} if one
   * is not found
   */
  public static DataSourceFactory forName(String name) {
    return null;
  }

  /**
   * Returns a new {@link DataSource} builder.
   *
   * @return a {@link DataSource} builder. Not {@code null}.
   */
  public nl.topicus.java.sql2.DataSource.Builder builder();

  /**
   * Name by which this factory is registered. By convention {@link DataSourceFactory}
   * names follow the same rules as Java package names.
   *
   * @return the name of this factory
   */
  public String getName();
}
