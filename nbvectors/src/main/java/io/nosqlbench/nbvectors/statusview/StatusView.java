package io.nosqlbench.nbvectors.statusview;

import io.nosqlbench.nbvectors.IndexedFloatVector;
import io.nosqlbench.nbvectors.NeighborhoodComparison;

public interface StatusView extends AutoCloseable{
  void onStart(int totalQueryVectors);

  void onChunk(int chunk, int chunkSize, int totalTrainingVectors);
  void onQueryVector(IndexedFloatVector vector, long index, long end);
  void onNeighborhoodComparison(NeighborhoodComparison comparison);
  void end();
}
