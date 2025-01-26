package io.nosqlbench.nbvectors.statusview;

import io.nosqlbench.nbvectors.datatypes.IndexedFloatVector;
import io.nosqlbench.nbvectors.computation.NeighborhoodComparison;

public class StatusViewNoOp implements StatusView {
  @Override
  public void onStart(int totalQueryVectors) {

  }

  @Override
  public void onChunk(int chunk, int chunkSize, int totalTrainingVectors) {
  }

  @Override
  public void onQueryVector(IndexedFloatVector vector, long index, long end) {
  }

  @Override
  public void onNeighborhoodComparison(NeighborhoodComparison comparison) {
  }

  @Override
  public void end() {
  }

  @Override
  public void close() throws Exception {
  }
}
