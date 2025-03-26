package io.nosqlbench.nbvectors.spec.access.datasets.types;

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


import io.nosqlbench.nbvectors.spec.VectorData;

/// Each dataset which is accessible through the {@link VectorData}
/// API **must** implement this interface.
/// @see VectorData
/// @param <T> the type of the vector elements
public interface DatasetView<T> {

  /// get the number of vectors in the dataset
  /// @return the number of vectors in the dataset
  public int getCount();
  /// get the number of dimensions in each vector
  /// @return the number of dimensions in each vector
  public int getVectorDimensions();
  /// get the base type of the vector elements
  /// @return the base type of the vector elements
  public Class<?> getBaseType();
  /// get a vector by its ordinal
  /// @param index the ordinal of the vector to get
  /// @return the vector
  public T get(long index);
  /// get a range of vectors by their ordinals
  /// @param startInclusive the first ordinal to get
  /// @param endExclusive the last ordinal to get
  /// @return the vectors
  public T[] getRange(long startInclusive, long endExclusive);
  /// get a vector by its ordinal
  /// @param index the ordinal of the vector to get
  /// @return the vector
  public Indexed<T> getIndexed(long index);
  /// get a range of vectors by their ordinals
  /// @param startInclusive the first ordinal to get
  /// @param endExclusive the last ordinal to get
  /// @return the vectors
  Indexed<T>[] getIndexedRange(long startInclusive, long endExclusive);
}
