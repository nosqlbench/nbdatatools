package io.nosqlbench.vectordata.internalapi.datasets.views;

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

import io.nosqlbench.vectordata.internalapi.datasets.IntVectors;

/// A view of data consisting of neighbor indices
public interface NeighborIndices extends IntVectors {
  /// get the maximum number of neighbors provided for each query vector
  /// @return the maximum number of neighbors provided for each query vector
  int getMaxK();
}
