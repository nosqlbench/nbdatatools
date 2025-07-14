package io.nosqlbench.command.hdf5.subcommands.build_hdf5.datasource;

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


import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;

/// A supplier for arrays of objects which are chunked into arrays of the specified size
/// @param <T> the type of the array elements
public class ArrayChunkingIterable<T> implements Iterable {

  private final Iterable<T> aryIterable;
  private final int maxsize;
  private final Class<T> clazz;

  /// create a supplier for arrays of objects which are chunked into arrays of the specified size
  /// @param clazz the class of the array elements
  /// @param aryIter the iterator of the array elements
  /// @param maxsize the maximum size of the array elements in bytes ; the chunk size limit
  public ArrayChunkingIterable(Class<T> clazz, Iterable<T> aryIter, int maxsize) {
    this.clazz = clazz;
    this.aryIterable = aryIter;
    this.maxsize = maxsize;
  }

  /// {@inheritDoc}
  @Override
  public Iterator iterator() {
    return new Iter(clazz, aryIterable.iterator(), maxsize, Long.MAX_VALUE);
  }

  /// An iterator for arrays of objects which are chunked into arrays of the specified size
  /// @param <T> the type of the array elements
  public static class Iter<T> implements Iterator<T> {

    private final Class<T> clazz;
    private final Iterator<T> iter;
    private final int maxsize;
    private final long limit = Long.MAX_VALUE;
    private long total = 0L;

    /// create an iterator for arrays of objects which are chunked into arrays of the specified size
    /// @param clazz the class of the array elements
    /// @param iter the iterator of the array elements
    /// @param maxsize the maximum size of the array elements in bytes ; the chunk size limit
    /// @param limit the maximum number of elements to return
    public Iter(Class<T> clazz, Iterator<T> iter, int maxsize, long limit) {
      this.clazz = clazz;
      this.iter = iter;
      this.maxsize = maxsize;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext() && total < limit;
    }

    @Override
    public T next() {
      T elem = iter.next();
      int size = sizeOf(elem);

      int remaining = (maxsize/size);
      T[] ary = (T[]) Array.newInstance(clazz, remaining);
      ary[0]=elem;
      int i;
      for (i = 1; i < ary.length; i++) {
        if (iter.hasNext() && total < limit) {
          ary[i]=iter.next();
          total++;
        } else {
          break;
        }
      }
      T[] ts = Arrays.copyOfRange(ary, 0, i);
      return (T) ts;
    }

    private int sizeOf(Object row0) {
      if (row0 instanceof int[] ia) {
        return ia.length * Integer.BYTES;
      } else if (row0 instanceof byte[] ba) {
        return ba.length * Byte.BYTES;
      } else if (row0 instanceof short[] sa) {
        return sa.length * Short.BYTES;
      } else if (row0 instanceof long[] la) {
        return la.length * Long.BYTES;
      } else if (row0 instanceof float[] fa) {
        return fa.length * Float.BYTES;
      } else if (row0 instanceof double[] da) {
        return da.length * Double.BYTES;
      } else {
        throw new RuntimeException("Unknown type for sizing:" + row0.getClass());
      }
    }

  }


}
