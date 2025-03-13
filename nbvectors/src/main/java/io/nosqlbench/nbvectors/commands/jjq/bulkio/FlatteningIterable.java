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

/// An [Iterable] of [O], wrapping the given [Iterable] of [I]
/// and function to convert an [I] -> [Iterable] of [O]
/// @param <I> the input type
/// @param <O> the output type
public class FlatteningIterable<I, O> implements Iterable<O> {
  private final Iterable<I> inner;
  private final Function<I, Iterable<O>> function;

  /// create a new flattening iterable
  /// @param inner The source iterable
  public FlatteningIterable(Iterable<I> inner) {
    this.function = (I i) -> (Iterable<O>) i;
    this.inner = inner;
  }

  /// create a new flattening iterable
  /// @param inner The source iterable
  /// @param function The function to convert an [I] to an [Iterable] of [O]
  public FlatteningIterable(Iterable<I> inner, Function<I, Iterable<O>> function) {
    this.inner = inner;
    this.function = function;
  }

  /// get the result type iterator
  @Override
  public Iterator<O> iterator() {
    return new FlatteningIterator<>(inner, function);
  }

  /// An [Iterator] of [O], wrapping the given [Iterator] of [I]
  /// @param <I> the input type
  /// @param <O> the output type
  public static class FlatteningIterator<I, O> implements Iterator<O>, DiagToString {
    private long inners, outers, totals;

    private final Function<I, Iterable<O>> function;
    private final Iterator<I> inputIter;
    private Iterator<O> outputIter;

    /// create a new flattening iterator
    /// @param innerIterable The source iterable
    /// @param function The function to convert an [I] to an [Iterable] of [O]
    public FlatteningIterator(Iterable<I> innerIterable, Function<I, Iterable<O>> function) {
      this.function = function;
      this.inputIter = innerIterable.iterator();
    }

    @Override
    public boolean hasNext() {
      return (outputIter != null && outputIter.hasNext()) || inputIter.hasNext();
    }

    @Override
    public O next() {
      while (outputIter == null || (!outputIter.hasNext() && inputIter.hasNext())) {
        I nextInner = inputIter.next();
        inners++;
        outers = 0;
        outputIter = function.apply(nextInner).iterator();
      }
      outers++;
      totals++;
      return outputIter.next();
    }

    @Override
    public String toDiagString() {
      return "inners:" + inners + ", outers:" + outers + ", totals:" + totals;
    }
  }
}
