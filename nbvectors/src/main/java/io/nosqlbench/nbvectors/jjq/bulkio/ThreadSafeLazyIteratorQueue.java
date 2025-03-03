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

public class ThreadSafeLazyIteratorQueue<T> extends AbstractQueue<T> {
  private final Iterator<T> iterator;
  private T nextElement;
  private boolean nextElementFetched = false;
  private final Object lock = new Object();

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

  /**
   * Returns the next element without removing it, or {@code null} if none remains.
   */
  @Override
  public T peek() {
    synchronized (lock) {
      ensureNext();
      return nextElementFetched ? nextElement : null;
    }
  }

  /**
   * Retrieves and removes the next element, or returns {@code null} if none remains.
   */
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

  /**
   * Adding new elements is not supported in this read-only adapter.
   */
  @Override
  public boolean offer(T t) {
    throw new UnsupportedOperationException("offer() is not supported in ThreadSafeLazyIteratorQueue");
  }

  /**
   * Iterator access is intentionally disabled to prevent exposing the underlying iterator.
   */
  @Override
  public Iterator<T> iterator() {
    throw new UnsupportedOperationException("Iterator access is not supported for ThreadSafeLazyIteratorQueue");
  }

  /**
   * Size cannot be determined without fully buffering the underlying iterator.
   */
  @Override
  public int size() {
    throw new UnsupportedOperationException("size() is not supported for ThreadSafeLazyIteratorQueue");
  }
}
