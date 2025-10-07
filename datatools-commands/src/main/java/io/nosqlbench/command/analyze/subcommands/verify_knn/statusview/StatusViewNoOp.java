package io.nosqlbench.command.analyze.subcommands.verify_knn.statusview;

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


import io.nosqlbench.command.analyze.subcommands.verify_knn.computation.NeighborhoodComparison;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;

/// A no-operation implementation of StatusView that does nothing when methods are called.
/// This is useful when status updates are not needed or should be suppressed.
public class StatusViewNoOp implements StatusView {

  /// Default constructor for StatusViewNoOp
  public StatusViewNoOp() {
  }

  @Override
  public void onStart(int totalQueryVectors) {
  }

  @Override
  public void onChunk(int chunk, int chunkSize, int totalTrainingVectors) {
  }

  @Override
  public void onQueryVector(Indexed<float[]> vector, long index, long end) {
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
