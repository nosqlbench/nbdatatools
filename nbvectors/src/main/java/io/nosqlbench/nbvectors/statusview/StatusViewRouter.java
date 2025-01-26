package io.nosqlbench.nbvectors.statusview;

import io.nosqlbench.nbvectors.datatypes.IndexedFloatVector;
import io.nosqlbench.nbvectors.computation.NeighborhoodComparison;

import java.util.ArrayList;
import java.util.List;

public class StatusViewRouter implements StatusView {
  private final List<StatusView> statusViewList = new ArrayList<>();

  public StatusViewRouter add(StatusView statusView) {
    statusViewList.add(statusView);
    return this;
  }

  @Override
  public void onStart(int totalQueryVectors) {
    for (StatusView view : statusViewList) {
      view.onStart(totalQueryVectors);
    }
  }

  @Override
  public void onChunk(int chunk, int chunkSize, int totalTrainingVectors) {
    for (StatusView view : statusViewList) {
      view.onChunk(chunk, chunkSize, totalTrainingVectors);
    }
  }

  @Override
  public void onQueryVector(IndexedFloatVector vector, long index, long end) {
    for (StatusView view : statusViewList) {
      view.onQueryVector(vector, index, end);
    }
  }

  @Override
  public void onNeighborhoodComparison(NeighborhoodComparison comparison) {
    for (StatusView view : statusViewList) {
      view.onNeighborhoodComparison(comparison);
    }
  }

  @Override
  public void end() {
    for (StatusView view : statusViewList) {
      view.end();
    }
  }

  @Override
  public void close() throws Exception {
    for (StatusView view : statusViewList) {
      view.close();
    }
  }

  public boolean isEmpty() {
    return statusViewList.isEmpty();
  }
}
