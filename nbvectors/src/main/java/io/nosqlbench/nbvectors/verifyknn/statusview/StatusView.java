package io.nosqlbench.nbvectors.verifyknn.statusview;

import io.nosqlbench.nbvectors.verifyknn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.verifyknn.computation.NeighborhoodComparison;

public interface StatusView extends AutoCloseable{
  void onStart(int totalQueryVectors);

  void onChunk(int chunk, int chunkSize, int totalTrainingVectors);
  void onQueryVector(LongIndexedFloatVector vector, long index, long end);
  void onNeighborhoodComparison(NeighborhoodComparison comparison);
  void end();
}
