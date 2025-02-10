package io.nosqlbench.nbvectors.jjq.bulkio;

import java.util.Iterator;
import java.util.function.Function;

/// An [Iterable] of [O], wrapping the given [Iterable] of [I]
/// and function to convert an [I] -> [Iterable] of [O]
public class FlatteningIterable<I, O> implements Iterable<O> {
  private final Iterable<I> inner;
  private final Function<I, Iterable<O>> function;

  public FlatteningIterable(Iterable<I> inner) {
    this.function = (I i) -> (Iterable<O>) i;
    this.inner = inner;
  }

  public FlatteningIterable(Iterable<I> inner, Function<I, Iterable<O>> function) {
    this.inner = inner;
    this.function = function;
  }

  @Override
  public Iterator<O> iterator() {
    return new FlatteningIterator<I, O>(inner, function);
  }

  public static class FlatteningIterator<I, O> implements Iterator<O>, DiagToString {
    private long inners, outers, totals;

    private final Function<I, Iterable<O>> function;
    private final Iterator<I> inputIter;
    private Iterator<O> outputIter;

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
      O next = outputIter.next();
      return next;
    }

    @Override
    public String toDiagString() {
      return "inners:" + inners + ", outers:" + outers + ", totals:" + totals;
    }
  }
}
