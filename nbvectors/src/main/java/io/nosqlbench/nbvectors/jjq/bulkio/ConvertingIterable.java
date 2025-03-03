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


import java.util.Iterator;
import java.util.function.Function;

public class ConvertingIterable<I,O> implements Iterable<O> {

  private final Iterable<I> inner;
  private final Function<I, O> converter;

  public ConvertingIterable(Iterable<I> inner, Function<I,O> converter) {
    this.inner = inner;
    this.converter = converter;
  }

  public
  @Override
  Iterator<O> iterator() {
    return new WrappedIterator(inner.iterator(), converter);
  }
  public static class WrappedIterator<I,O> implements Iterator<O> {

    private final Iterator<I> innerIter;
    private final Function<I, O> converter;

    public WrappedIterator(Iterator<I> inner, Function<I,O> converter) {
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
