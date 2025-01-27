package io.nosqlbench.verifyknn.statusview;

import io.nosqlbench.verifyknn.datatypes.IndexedFloatVector;
import io.nosqlbench.verifyknn.computation.NeighborhoodComparison;

public interface StatusView extends AutoCloseable{
  void onStart(int totalQueryVectors);

  void onChunk(int chunk, int chunkSize, int totalTrainingVectors);
  void onQueryVector(IndexedFloatVector vector, long index, long end);
  void onNeighborhoodComparison(NeighborhoodComparison comparison);
  void end();
}
