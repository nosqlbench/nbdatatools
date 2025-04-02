package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet.traversal;

import io.nosqlbench.nbvectors.commands.jjq.bulkio.iteration.ConcatenatingIterable;

import java.util.Iterator;

public class CompositeVectorsIterable implements Iterable<float[]> {

  ConcatenatingIterable<float[]> concat;

  public CompositeVectorsIterable(Iterable<Iterable<float[]>> iterables) {
     concat = new ConcatenatingIterable<>(iterables);
  }

  @Override
  public Iterator<float[]> iterator() {
    return concat.iterator();
  }
}
