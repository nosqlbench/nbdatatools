package io.nosqlbench.command.verify_knn.statusview;

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

import io.nosqlbench.command.verify_knn.CMD_verify_knn;
import io.nosqlbench.command.verify_knn.computation.NeighborhoodComparison;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;

/// status view eventing interface for [CMD_verify_knn]
public interface StatusView extends AutoCloseable{

  /// startInclusive of verification
  /// @param totalQueryVectors the total number of query vectors to be tested
  void onStart(int totalQueryVectors);

  /// a chunk of training vectors has been loaded
  /// @param chunk the chunk number
  /// @param chunkSize the number of training vectors in this chunk
  /// @param totalTrainingVectors the total number of training vectors
  void onChunk(int chunk, int chunkSize, int totalTrainingVectors);

  /// a query vector has been loaded
  /// @param vector the query vector
  /// @param index the index of the query vector in the test data
  /// @param end the end index of the test data
  void onQueryVector(Indexed<float[]> vector, long index, long end);

  /// a neighborhood comparison has been performed
  /// @param comparison the neighborhood comparison
  void onNeighborhoodComparison(NeighborhoodComparison comparison);

  /// end of verification
  void end();
}
