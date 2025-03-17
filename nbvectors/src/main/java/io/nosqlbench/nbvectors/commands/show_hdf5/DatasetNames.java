package io.nosqlbench.nbvectors.commands.show_hdf5;

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

/// These are the dataset names that are part of the vector test data format
public enum DatasetNames {

  /// The training dataset, containing all vectors to search, organized by index
  train,
  /// The test dataset, containing all query vectors, organized by index
  test,
  /// The neighbors dataset, containing all correct KNN results, organized by index
  neighbors,
  /// The distances dataset, containing all correct distances, organized by index
  distances,
  /// The filter dataset, containing all query predicates, organized by index
  filters
}
