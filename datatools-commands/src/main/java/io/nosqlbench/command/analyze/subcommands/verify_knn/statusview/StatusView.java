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

/// Interface for displaying status information during KNN verification process
public interface StatusView extends AutoCloseable{

  /// Start of verification process
  /// @param totalQueryVectors the total number of query vectors to be tested
  void onStart(int totalQueryVectors);

  /// Called when a chunk of training vectors has been loaded
  /// @param chunk the chunk number
  /// @param chunkSize the number of training vectors in this chunk
  /// @param totalTrainingVectors the total number of training vectors
  void onChunk(int chunk, int chunkSize, int totalTrainingVectors);

  /// Called when a query vector has been loaded
  /// @param vector the query vector
  /// @param index the index of the query vector in the test data
  /// @param end the end index of the test data
  void onQueryVector(Indexed<float[]> vector, long index, long end);

  /// Called when a neighborhood comparison has been performed
  /// @param comparison the neighborhood comparison
  void onNeighborhoodComparison(NeighborhoodComparison comparison);

  /// Called at the end of verification process
  void end();
}
