package io.nosqlbench.nbvectors.spec.access.datasets.impl;

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


import io.jhdf.api.Dataset;
import io.nosqlbench.nbvectors.spec.access.datasets.types.DatasetView;
import io.nosqlbench.nbvectors.spec.access.datasets.types.Indexed;

/// This class provides the core implementation of the {@link DatasetView} interface.
/// It is meant to be extended to implement domain-typed interfaces. You can simply expose or
/// reuse the methods here as needed.
/// @param <T>
///     the type of the elements in the dataset
/// @see DatasetView
public class CoreDatasetViewMethods<T> implements DatasetView<T> {
  protected final Dataset dataset;

  /// create a dataset view
  /// @param dataset the dataset to view
  public CoreDatasetViewMethods(Dataset dataset) {
    this.dataset = dataset;
  }

  /// get the number of vectors in the dataset
  /// @return the number of vectors in the dataset
  public int getCount() {
    return dataset.getDimensions()[0];
  }

  /// get the number of dimensions in each vector
  /// @return the number of dimensions in each vector
  public int getVectorDimensions() {
    return dataset.getDimensions()[1];
  }

  /// get the base type of the vector elements
  /// @return the base type of the vector elements
  public Class<? extends Number> getBaseType() {
    Class<?> type = dataset.getDataType().getJavaType();
    while (type.isArray()) {
      type = type.getComponentType();
    }
    return (Class<? extends Number>) type;
  }

  /// get a vector by its ordinal
  /// @param index the ordinal of the vector to get
  /// @return the vector
  protected float[] getFloatVector(long index) {
    Object data = getObject(index);
    return (float[]) data;
  }

  /// get a vector by its ordinal
  /// @param index the ordinal of the vector to get
  /// @return the vector
  public T getVector(long index) {
    return slice(index);
  }

  /// get a range of vectors by their ordinals
  /// @param startInclusive the ordinal of the first vector to get
  /// @param endExclusive the ordinal of the last vector to get
  /// @return the vectors
  public T[] getVectors(long startInclusive, long endExclusive) {
    return sliceRange(startInclusive, endExclusive);
  }

  /// get a vector by its ordinal
  /// @param index the ordinal of the vector to get
  /// @return the vector
  protected double[] getDoubleVector(long index) {
    Object data = getObject(index);
    return (double[]) data;
  }

  /// get a vector by its ordinal
  /// @param index the ordinal of the vector to get
  /// @return the vector
  protected Indexed<T> getIndexedObject(long index) {
    T data = slice(index);
    return new Indexed<>(index, data);
  }

  /// get a range of vectors by their ordinals
  /// @param startInclusive the ordinal of the first vector to get
  /// @param endExclusive the ordinal of the last vector to get
  /// @return the vectors
  protected Object[] getDataRange(long startInclusive, long endExclusive) {
    int[] dims = dataset.getDimensions();
    long sliceRows = endExclusive - startInclusive;
    if (sliceRows > Integer.MAX_VALUE) {
      throw new RuntimeException("Slice is too large to fit in an array: " + sliceRows);
    }
    dims[0] = (int) sliceRows;
    long[] offsets = new long[dims.length];
    offsets[0] = startInclusive;
    return (Object[]) dataset.getData(offsets, dims);
  }

  /// get an object by its ordinal
  /// @param index the ordinal of the object to get
  /// @return the object
  protected Object getObject(long index) {
    int[] dims = dataset.getDimensions();
    dims[0] = 1;
    long[] offsets = new long[dims.length];
    offsets[0] = index;
    return dataset.getData(offsets, dims);
  }

  /// get an object by its ordinal
  /// @param index the ordinal of the object to get
  /// @return the object
  protected T slice(long index) {
    return (T) getObject(index);
  }

  /// get a range of objects by their ordinals
  /// @param startInclusive the ordinal of the first object to get
  /// @param endExclusive the ordinal of the last object to get
  /// @return the objects
  protected Indexed<T>[] sliceIndexed1D(long startInclusive, long endExclusive) {
    T[] slices = sliceRange(startInclusive, endExclusive);
    Indexed<T>[] indexed = new Indexed[slices.length];
    for (int i = 0; i < slices.length; i++) {
      indexed[i] = new Indexed<>(startInclusive + i, slices[i]);
    }
    return indexed;
  }

  /// get a range of objects by their ordinals
  /// @param startInclusive the ordinal of the first object to get
  /// @param endExclusive the ordinal of the last object to get
  /// @return the objects
  protected T[] sliceRange(long startInclusive, long endExclusive) {
    return (T[]) getDataRange(startInclusive, endExclusive);
  }

  /// get an object by its ordinal
  /// @param ordinal the ordinal of the object to get
  /// @return the object
  public T get(long ordinal) {
    return slice(ordinal);
  }

  /// get a range of objects by their ordinals
  /// @param startInclusive the ordinal of the first object to get
  /// @param endExclusive the ordinal of the last object to get
  /// @return the objects
  public T[] getRange(long startInclusive, long endExclusive) {
    return sliceRange(startInclusive, endExclusive);
  }

  /// get an object by its ordinal
  /// @param index the ordinal of the object to get
  /// @return the object
  @Override
  public Indexed<T> getIndexed(long index) {
    T t = get(index);
    return new Indexed<>(index, t);
  }

  /// get a range of objects by their ordinals
  /// @param startInclusive the ordinal of the first object to get
  /// @param endExclusive the ordinal of the last object to get
  /// @return the objects
  @Override
  public Indexed<T>[] getIndexedRange(long startInclusive, long endExclusive) {
    T[] range = getRange(startInclusive, endExclusive);
    if (range != null) {
      Indexed<T>[] indexed = new Indexed[range.length];
      for (int i = 0; i < range.length; i++) {
        indexed[i] = new Indexed<>(startInclusive + i, range[i]);
      }
      return indexed;
    }
    return new Indexed[0];
  }


}
