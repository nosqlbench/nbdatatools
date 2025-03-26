package io.nosqlbench.nbvectors.spec;

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
import io.nosqlbench.nbvectors.spec.access.datasets.impl.IntVectorsImpl;
import io.nosqlbench.nbvectors.spec.access.datasets.types.NeighborIndices;

/// A view of neighbor indices data
public class NeighborIndicesImpl extends IntVectorsImpl implements NeighborIndices {

  /// create a new neighbor indices view
  /// @param dataset the dataset to view
  public NeighborIndicesImpl(Dataset dataset) {
    super(dataset);
  }

  /// {@inheritDoc}
  @Override
  public int getMaxK() {
    return dataset.getDimensions()[1];
  }
}
