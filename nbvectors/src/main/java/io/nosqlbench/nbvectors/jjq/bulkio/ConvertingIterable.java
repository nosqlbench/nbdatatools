package io.nosqlbench.nbvectors.jjq.bulkio;

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
      return converter.apply(innerIter.next());
    }
  }
}
