package io.nosqlbench.vectordata.spec.datasets.impl.hdf5;

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
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;
import io.nosqlbench.vectordata.spec.datasets.types.DatasetView;
import io.nosqlbench.vectordata.layout.FWindow;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.IntStream;

/// This class provides the core implementation of the {@link DatasetView} interface.
/// It is meant to be extended to implement domain-typed interfaces. You can simply expose or
/// reuse the methods here as needed.
/// @param <T>
///     the type of the elements in the dataset
///
///     ---
///
///     ### Data type semantics
///
///     The data type semantics of this layer are have some nuances which are important to
///     obvserver.
///     Overall, this an access interface to _vector_ data, or in simple Java parlance, array data.
///     That means that the base type of each element is an array type as the user sees it. Thus,
///     the
///      type T will be a proper Java array type like (float[], int[], byte[] or whatever).
///     **Additionally**, the data is stored in the backing store as a homogeneous array of those
///     types, such as (float[][], int[][], byte[][] and similar). Since generic type tokens do not
///     (
///     yet) extend richly to array types and liksov relationships, we have to be slightly careful
///     about
///     how this is expressed. So the base [T] type is not stated in terms of an array, but as an
///     unqualified type. Internally, this is understood to be an two-dimensional array at the base
///     structure, but it **IS** possible that this is multi-dimensional array, wherein each [T]
///     element is itself a multi-dimensional array. What matters is that the major dimension is
///     the
///     element selector, the minor dimension is the vector component selector, and any interior
///     dimensions are carried along without need for further type introspection.
///
///     ---
///
///     ### Rationalizing dimensions
///
///     Where a window is specified, it specifies a view into the raw data array across dimensions.
///     The window doesn't have to have the same dimensionality as the underlying backing data.
///     Only
///     the dimensions which are asserted are used, and once asserted, the window is referenced as
///     the one and true shape value for the user. For now, window assertions of more than 2
///     dimensions are not defined or supported.
///
///     The special window [FWindow#ALL] is a sigil value which means that the window, upon
///     application to the underlying dataset, should simply be sized to the underlying dataset
///     upon
///     initialization.
/// @see DatasetView
public abstract class CoreHdf5DatasetViewMethods<T> implements DatasetView<T> {

  /// the dataset to wrap
  protected final Dataset dataset;
  private final FWindow window;

  /// create a dataset view
  /// @param dataset
  ///     the dataset to view
  /// @param window
  /// The window to validate, can be FWindow.ALL for full dataset
  public CoreHdf5DatasetViewMethods(Dataset dataset, FWindow window) {
    this.dataset = dataset;
    this.window = validateWindow(window);
  }


  /// Validates and adjusts the provided window against dataset dimensions
  /// @param window
  ///     The window to validate, can be FWindow.ALL for full dataset
  /// @return A validated FWindow that fits within dataset dimensions
  /// @throws RuntimeException
  ///     If window dimensions exceed the dataset dimensions
  protected FWindow validateWindow(FWindow window) {
    FWindow valid = window;
    if (FWindow.ALL.equals(valid)) {
      valid = new FWindow("0.." + dataset.getDimensions()[0]);
    } else {
      int[] dimensions = dataset.getDimensions();
      int checkDims = Math.min(dimensions.length, valid.intervals().size());
      for (int i = 0; i < checkDims; i++) {
        if (valid.intervals().get(i).count() > dimensions[i]) {
          throw new RuntimeException("window exceeds dataset dimensions at dimension " + i);
        }
      }
    }
    return valid;
  }

  /// get the number of vectors in the dataset
  /// @return the number of vectors in the dataset
  public int getCount() {
    return window.intervals().get(0).count();
  }

  /// get the number of dimensions in each vector
  /// @return the number of dimensions in each vector
  public int getVectorDimensions() {
    if (window.intervals().size() >= 2) { // asserted access window at dimension 2, pre-checked
      return window.intervals().get(1).count();
    }
    return window.intervals().get(0).count();
  }

  /// get the base type of the vector elements
  /// @return the base type of the vector elements
  public Class<? extends Number> getDataType() {
    Class<?> type = dataset.getDataType().getJavaType();
    while (type.isArray()) {
      type = type.getComponentType();
    }
    return (Class<? extends Number>) type;
  }

  /// get a vector by its ordinal
  /// @param index
  ///     the ordinal of the vector to get
  /// @return the vector
  protected float[] getFloatVector(long index) {
    Object data = getRawElement(index);
    return (float[]) data;
  }

  /// get a vector by its ordinal
  /// @param index
  ///     the ordinal of the vector to get
  /// @return the vector
  public T getVector(long index) {
    return slice(index);
  }

  /// get a range of vectors by their ordinals
  /// @param startInclusive
  ///     the ordinal of the first vector to get
  /// @param endExclusive
  ///     the ordinal of the last vector to get
  /// @return the vectors
  public T[] getVectors(long startInclusive, long endExclusive) {
    T[] ts = sliceRange(startInclusive, endExclusive);
    return ts;
  }

  /// get a vector by its ordinal
  /// @param index
  ///     the ordinal of the vector to get
  /// @return the vector
  protected double[] getDoubleVector(long index) {
    Object data = getRawElement(index);
    return (double[]) data;
  }

  /// get a vector by its ordinal
  /// @param index
  ///     the ordinal of the vector to get
  /// @return the vector
  protected Indexed<T> getIndexedObject(long index) {
    T data = slice(index);
    return new Indexed<>(index, data);
  }

  /// get a range of vectors by their ordinals
  /// @param startInclusive
  ///     the ordinal of the first vector to get
  /// @param endExclusive
  ///     the ordinal of the last vector to get
  /// @return the vectors
  protected Object[] getDataRange(long startInclusive, long endExclusive) {
    startInclusive = window.translate(startInclusive);
    endExclusive = window.translate(endExclusive);

    int[] dims = dataset.getDimensions();
    long sliceRows = endExclusive - startInclusive;
    if (sliceRows > Integer.MAX_VALUE) {
      throw new RuntimeException("Slice is too large to fit in an array: " + sliceRows);
    }
    dims[0] = (int) sliceRows;
    long[] offsets = new long[dims.length];
    offsets[0] = startInclusive;

    Object data = dataset.getData(offsets, dims);
    return (Object[]) data;
  }

  /// get an object by its ordinal
  /// @param index
  ///     the ordinal of the object to get
  /// @return the object
  protected Object getRawElement(long index) {
    index = window.translate(index);
    int[] dims = dataset.getDimensions();
    dims[0] = 1;
    long[] offsets = new long[dims.length];
    offsets[0] = index;
    Object data = dataset.getData(offsets, dims);
    return Array.get(data, 0);
  }

  /// get an object by its ordinal
  /// @param index
  ///     the ordinal of the object to get
  /// @return the object
  protected T slice(long index) {
    return (T) getRawElement(index);
  }

  /// get a range of objects by their ordinals
  /// @param startInclusive
  ///     the ordinal of the first object to get
  /// @param endExclusive
  ///     the ordinal of the last object to get
  /// @return the objects
  public Indexed<T>[] sliceIndexed1D(long startInclusive, long endExclusive) {
    T[] slices = sliceRange(startInclusive, endExclusive);
    Indexed<T>[] indexed = new Indexed[slices.length];
    for (int i = 0; i < slices.length; i++) {
      indexed[i] = new Indexed<>(startInclusive + i, slices[i]);
    }
    return indexed;
  }

  /// get a range of objects by their ordinals
  /// @param startInclusive
  ///     the ordinal of the first object to get
  /// @param endExclusive
  ///     the ordinal of the last object to get
  /// @return the objects
  protected T[] sliceRange(long startInclusive, long endExclusive) {
    return (T[]) getDataRange(startInclusive, endExclusive);
  }

  /// get an object by its ordinal
  /// @param ordinal
  ///     the ordinal of the object to get
  /// @return the object
  public T get(long ordinal) {
    T slice = slice(ordinal);
    return slice;
  }

  /// Retrieves an object at the specified ordinal asynchronously.
  ///
  /// This method provides an asynchronous version of get(long ordinal).
  /// Since the underlying synchronous get method is already implemented,
  /// this returns a completed future with the result.
  ///
  /// @param index 
  ///     The ordinal of the object to retrieve
  /// @return A Future containing the object at the specified ordinal
  @Override
  public Future<T> getAsync(long index) {
    return CompletableFuture.completedFuture(get(index));
  }

  /// get a range of objects by their ordinals
  /// @param startInclusive
  ///     the ordinal of the first object to get
  /// @param endExclusive
  ///     the ordinal of the last object to get
  /// @return the objects
  public T[] getRange(long startInclusive, long endExclusive) {
    T[] ts = sliceRange(startInclusive, endExclusive);
    return ts;
  }

  /// Retrieves a range of objects by their ordinals asynchronously.
  ///
  /// This method provides an asynchronous version of getRange(long, long).
  /// Since the underlying synchronous getRange method is already implemented,
  /// this returns a completed future with the result.
  ///
  /// @param startInclusive 
  ///     The ordinal of the first object to get
  /// @param endExclusive 
  ///     The ordinal of the last object to get
  /// @return A Future containing an array of objects in the specified range
  @Override
  public Future<T[]> getRangeAsync(long startInclusive, long endExclusive) {
    return CompletableFuture.completedFuture(getRange(startInclusive, endExclusive));
  }

  /// get an object by its ordinal
  /// @param index
  ///     the ordinal of the object to get
  /// @return the object
  @Override
  public Indexed<T> getIndexed(long index) {
    T t = get(index);
    Indexed<T> tIndexed = new Indexed<>(index, t);
    return tIndexed;
  }

  /// Retrieves an object at the specified ordinal, wrapped in an Indexed container, asynchronously.
  ///
  /// This method provides an asynchronous version of getIndexed(long index).
  /// Since the underlying synchronous getIndexed method is already implemented,
  /// this returns a completed future with the result.
  ///
  /// @param index 
  ///     The ordinal of the object to retrieve
  /// @return A Future containing an Indexed object with the ordinal and data
  @Override
  public Future<Indexed<T>> getIndexedAsync(long index) {
    return CompletableFuture.completedFuture(getIndexed(index));
  }

  /// get a range of objects by their ordinals
  /// @param startInclusive
  ///     the ordinal of the first object to get
  /// @param endExclusive
  ///     the ordinal of the last object to get
  /// @return the objects
  @Override
  public Indexed<T>[] getIndexedRange(long startInclusive, long endExclusive) {
    Indexed<T>[] result = null;
    T[] range = getRange(startInclusive, endExclusive);
    if (range != null) {
      Indexed<T>[] indexed = new Indexed[range.length];
      for (int i = 0; i < range.length; i++) {
        indexed[i] = new Indexed<>(startInclusive + i, range[i]);
      }
      result = indexed;
    } else {
      result = new Indexed[0];
    }
    return result;
  }

  /// Retrieves a range of objects, each wrapped in an Indexed container, asynchronously.
  ///
  /// This method provides an asynchronous version of getIndexedRange(long, long).
  /// Since the underlying synchronous getIndexedRange method is already implemented,
  /// this returns a completed future with the result.
  ///
  /// @param startInclusive 
  ///     The ordinal of the first object to get
  /// @param endExclusive 
  ///     The ordinal of the last object to get
  /// @return A Future containing an array of Indexed objects for the specified range
  @Override
  public Future<Indexed<T>[]> getIndexedRangeAsync(long startInclusive, long endExclusive) {
    return CompletableFuture.completedFuture(getIndexedRange(startInclusive, endExclusive));
  }

  @Override
  public List<T> toList() {
    List<T> list = IntStream.range(0, getCount()).mapToObj(this::get).collect(java.util.stream.Collectors.toList());
    return list;
  }

  @Override
  public <U> List<U> toList(Function<T, U> f) {
    List<U> list = IntStream.range(0, getCount()).mapToObj(i -> f.apply(get(i))).collect(java.util.stream.Collectors.toList());
    return list;
  }

  @NotNull
  @Override
  public Iterator<T> iterator() {
    return new IteratorOf<>(this);
  }

  private final static class IteratorOf<T> implements Iterator<T> {
    private final DatasetView<T> dataset;
    private final PrimitiveIterator.OfInt iter;

    public IteratorOf(DatasetView<T> dataset) {
      this.iter = IntStream.range(0, dataset.getCount()).iterator();
      this.dataset = dataset;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public T next() {
      int nextidx = iter.next();
      return dataset.get(nextidx);
    }
  }

  @Override
  public CompletableFuture<Void> prebuffer() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> prebuffer(long startIncl, long endExcl) {
    return CompletableFuture.completedFuture(null);
  }


}
