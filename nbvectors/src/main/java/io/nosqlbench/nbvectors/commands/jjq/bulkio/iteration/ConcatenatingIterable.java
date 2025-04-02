package io.nosqlbench.nbvectors.commands.jjq.bulkio.iteration;

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


import io.nosqlbench.nbvectors.commands.jjq.bulkio.DiagToString;

import java.util.Iterator;

/// An [Iterable] of [O], comprised of a series [Iterable] of [Iterable] of [O].
/// @param <O>
///     the input type
public class ConcatenatingIterable<O> implements Iterable<O> {
  private final Iterable<Iterable<O>> sequence;

  /// create a new flattening iterable
  /// @param sequence The sequence of iterable stages to concatenate
  public ConcatenatingIterable(Iterable<Iterable<O>> sequence) {
    this.sequence = sequence;
  }

  /// get the result type iterator
  @Override
  public Iterator<O> iterator() {
    return new ConcatenatingIterator<>(sequence);
  }

  /// An [Iterator] of [O], wrapping the given [Iterator] of [O]
  /// @param <O>
  ///     the output type
  public static class ConcatenatingIterator<O> implements Iterator<O> {
    private long inners, outers, totals;

    private Iterator<Iterable<O>> sequenceIterator;
    private Iterator<O> stageIterator;

    public ConcatenatingIterator(Iterable<Iterable<O>> sequence) {
      this.sequenceIterator = sequence.iterator();
    }

    @Override
    public boolean hasNext() {
      while (true) {
        if (stageIterator == null || !stageIterator.hasNext()) {
          if (sequenceIterator.hasNext()) {
            stageIterator = sequenceIterator.next().iterator();
          } else {
            return false;
          }
        } else {
          return true;
        }
      }
    }

    @Override
    public O next() {
      return stageIterator.next();
    }
  }
}
