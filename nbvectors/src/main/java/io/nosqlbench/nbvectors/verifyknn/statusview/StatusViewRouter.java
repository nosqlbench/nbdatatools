package io.nosqlbench.nbvectors.verifyknn.statusview;

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


import io.nosqlbench.nbvectors.verifyknn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.verifyknn.computation.NeighborhoodComparison;

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
  public void onQueryVector(LongIndexedFloatVector vector, long index, long end) {
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
