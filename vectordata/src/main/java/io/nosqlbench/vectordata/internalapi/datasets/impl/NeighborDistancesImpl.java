package io.nosqlbench.vectordata.internalapi.datasets.impl;

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


import io.jhdf.api.Dataset;
import io.nosqlbench.vectordata.layout.FWindow;
import io.nosqlbench.vectordata.internalapi.datasets.FloatVectorsImpl;
import io.nosqlbench.vectordata.internalapi.datasets.views.NeighborDistances;
import io.nosqlbench.vectordata.internalapi.datasets.views.QueryVectors;

/// A view of neighbor indices data
public class NeighborDistancesImpl extends FloatVectorsImpl implements NeighborDistances {

  /// create a new neighbor indices view
  /// @param dataset the dataset to view
  /// @param window the window to use
  /// @see QueryVectors
  public NeighborDistancesImpl(Dataset dataset, FWindow window) {
    super(dataset, window);
  }

  @Override
  public int getMaxK() {
    return dataset.getDimensions()[1];
  }
}
