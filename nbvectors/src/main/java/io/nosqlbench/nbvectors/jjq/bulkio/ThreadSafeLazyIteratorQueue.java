package io.nosqlbench.nbvectors.jjq.bulkio;

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


import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;

/// A thread-safe queue adapter for an underlying iterator.
/// @param <T> the type of elements in the queue
public class ThreadSafeLazyIteratorQueue<T> extends AbstractQueue<T> {
  private final Iterator<T> iterator;
  private T nextElement;
  private boolean nextElementFetched = false;
  private final Object lock = new Object();

  /// create a thread-safe lazy iterator queue
  /// @param iterator the underlying iterator
  public ThreadSafeLazyIteratorQueue(Iterator<T> iterator) {
    if (iterator == null) {
      throw new NullPointerException("iterator cannot be null");
    }
    this.iterator = iterator;
  }

  /**
   * Fetches the next element from the underlying iterator if it hasn’t been fetched yet.
   * This method must be called from within a synchronized block on {@code lock}.
   */
  private void ensureNext() {
    if (!nextElementFetched && iterator.hasNext()) {
      nextElement = iterator.next();
      nextElementFetched = true;
    }
  }

  /// {@inheritDoc}
  @Override
  public T peek() {
    synchronized (lock) {
      ensureNext();
      return nextElementFetched ? nextElement : null;
    }
  }

  /// {@inheritDoc}
  @Override
  public T poll() {
    synchronized (lock) {
      ensureNext();
      if (!nextElementFetched) {
        return null;
      }
      T result = nextElement;
      nextElement = null;
      nextElementFetched = false;
      return result;
    }
  }

  /// {@inheritDoc}
  @Override
  public boolean offer(T t) {
    throw new UnsupportedOperationException("offer() is not supported in ThreadSafeLazyIteratorQueue");
  }


  /// Iterator access is intentionally disabled to prevent exposing the underlying iterator.
  /// @return an iterator for this queue
  @Override
  public Iterator<T> iterator() {
    throw new UnsupportedOperationException("Iterator access is not supported for ThreadSafeLazyIteratorQueue");
  }

  /// Size cannot be determined without fully buffering the underlying iterator.
  /// @return the size of this queue
  /// @throws UnsupportedOperationException every time
  @Override
  public int size() {
    throw new UnsupportedOperationException("size() is not supported for ThreadSafeLazyIteratorQueue");
  }
}
