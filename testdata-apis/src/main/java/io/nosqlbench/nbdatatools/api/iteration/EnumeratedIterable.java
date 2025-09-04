package io.nosqlbench.nbdatatools.api.iteration;

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


import io.nosqlbench.nbdatatools.api.types.Enumerated;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

/// An [Iterable] of [T] which is also [Enumerated]
/// @param <T> the type of the elements
public class EnumeratedIterable<T> implements Iterable<T> {
  private final Iterable<T> inner;

  /// create a new enumerated iterable
  /// @param inner the source iterable
  public EnumeratedIterable(Iterable<T> inner) {
    this.inner = inner;
  }

  /// get the result type iterator
  /// @return the result type iterator
  @NotNull
  @Override
  public Iterator<T> iterator() {
    return new EnumeratedIterator<T>(inner.iterator());
  }

  /// create a new enumerated iterable
  /// @param inner the source iterable
  /// @param <T> the type of the elements
  /// @return the new enumerated iterable
  public static <T> EnumeratedIterable<T> of(Iterable<T> inner) {
    if (inner instanceof EnumeratedIterable) {
      @SuppressWarnings("unchecked")
      EnumeratedIterable<T> ei = (EnumeratedIterable<T>) inner;
      return ei;
    } else {
      return new EnumeratedIterable<>(inner);
    }
  }

  private class EnumeratedIterator<T> implements Iterator<T>, Enumerated<T> {
    private final Iterator<T> iterator;
    private long lastIndex=-1;

    public EnumeratedIterator(Iterator<T> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public T next() {
      T next = iterator.next();
      lastIndex++;
      return next;
    }

    @Override
    public long getLastIndex() {
      return lastIndex;
    }
  }
}
