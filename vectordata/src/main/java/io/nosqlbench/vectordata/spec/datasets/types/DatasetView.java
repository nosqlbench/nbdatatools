package io.nosqlbench.vectordata.spec.datasets.types;

/*
 * Copyright (c) nosqlbench
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import io.nosqlbench.vectordata.discovery.TestDataGroup;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;

/// Each dataset which is accessible through the {@link TestDataGroup}
/// API **must** implement this interface.
/// @see TestDataGroup
/// @param <T> the type of the vector elements
public interface DatasetView<T> extends Iterable<T> {

  /// Asynchronously buffer an interval of bytes from the remote file to the local cache file and
  ///  return a future which can be used to block synchronously until it is avaialble.
  /// @param startIncl The starting position to buffer from
  /// @param endExcl The ending position to buffer to
  /// @return A future which can be used to block synchronously until it is available
  public CompletableFuture<Void> prebuffer(long startIncl, long endExcl);

  /// Asynchronously buffer the full range of the dataset view and
  /// return a future which can be used to block synchronously until it is available.
  /// This method prebuffers from 0 to getCount().
  /// @return A future which can be used to block synchronously until it is available
  public CompletableFuture<Void> prebuffer();

  /// get the number of vectors in the dataset
  /// @return the number of vectors in the dataset
  public int getCount();

  /// get the number of dimensions in each vector
  /// @return the number of dimensions in each vector
  public int getVectorDimensions();

  /// get the base type of the vector elements
  /// @return the base type of the vector elements
  public Class<?> getDataType();

  /// get a vector by its ordinal
  /// @param index the ordinal of the vector to get
  /// @return the vector
  public T get(long index);

  /// get a vector asynchronously
  /// @param index the ordinal of the vector to get
  /// @return a future for the vector
  public Future<T> getAsync(long index);

  /// get a range of vectors by their ordinals
  /// @param startInclusive the first ordinal to get
  /// @param endExclusive the last ordinal to get
  /// @return the vectors
  public T[] getRange(long startInclusive, long endExclusive);

  /// get a range of vectors asynchronously
  /// @param startInclusive the first ordinal to get
  /// @param endExclusive the last ordinal to get
  /// @return a future for the vectors
  public Future<T[]> getRangeAsync(long startInclusive, long endExclusive);

  /// get a vector by its ordinal
  /// @param index the ordinal of the vector to get
  /// @return the vector
  public Indexed<T> getIndexed(long index);

  /// get an indexed vector asynchronously
  /// @param index the ordinal of the vector to get
  /// @return a future for the vector
  public Future<Indexed<T>> getIndexedAsync(long index);

  /// get a range of vectors by their ordinals
  /// @param startInclusive the first ordinal to get
  /// @param endExclusive the last ordinal to get
  /// @return the vectors
  public Indexed<T>[] getIndexedRange(long startInclusive, long endExclusive);

  /// Get a range of indexed vectors asynchronously.
  /// @param startInclusive the first ordinal to get
  /// @param endExclusive the last ordinal to get
  /// @return a future for the vectors
  public Future<Indexed<T>[]> getIndexedRangeAsync(long startInclusive, long endExclusive);

  /// get a list of all vectors
  /// @return the list of all vectors
  public List<T> toList();

  /// get a list of all vectors, transformed by the given function
  /// @param f the function to transform the vectors
  /// @param <U> the type of the transformed vectors
  /// @return the list of all vectors, transformed
  public <U> List<U> toList(Function<T, U> f);

}
