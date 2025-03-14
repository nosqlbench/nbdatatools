package io.nosqlbench.nbvectors.commands.jjq.bulkio;

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


import java.util.Iterator;
import java.util.function.Function;

/// Convert an iterable of type {@link I} to an iterable of type {@link O},
/// doing translation in-flight as elements are accessed.
/// @param <I>
///     the input type
/// @param <O>
///     the output type
public class ConvertingIterable<I, O> implements Iterable<O> {

  private final Iterable<I> inner;
  private final Function<I, O> converter;

  /// create a converting iterable
  /// @param converter
  ///     a function to convert {@link I} to {@link O}
  /// @param inner
  ///     the source of {@link I} elements
  public ConvertingIterable(Iterable<I> inner, Function<I, O> converter) {
    this.inner = inner;
    this.converter = converter;
  }

  /// get the result type iterator
  /// @return an iterator of {@link O}
  @Override
  public Iterator<O> iterator() {
    return new WrappedIterator<>(inner.iterator(), converter);
  }

  /// create a new wrapped iterator
  /// @param <I> the input type
  /// @param <O> the output type
  public static class WrappedIterator<I, O> implements Iterator<O> {

    private final Iterator<I> innerIter;
    private final Function<I, O> converter;

    /// create a wrapped iterator
    /// @param converter the function to convert {@link I} to {@link O}
    /// @param inner the inner iterator for {@link I}
    public WrappedIterator(Iterator<I> inner, Function<I, O> converter) {
      this.innerIter = inner;
      this.converter = converter;
    }

    @Override
    public boolean hasNext() {
      return innerIter.hasNext();
    }

    @Override
    public O next() {
      I next = innerIter.next();
      O converted = converter.apply(next);
      return converted;
    }
  }
}
