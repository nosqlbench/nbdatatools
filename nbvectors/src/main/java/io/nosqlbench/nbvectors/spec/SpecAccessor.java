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


import io.jhdf.HdfFile;
import io.nosqlbench.nbvectors.spec.access.datasets.BaseVectors;
import io.nosqlbench.nbvectors.spec.access.datasets.NeighborDistances;
import io.nosqlbench.nbvectors.spec.access.datasets.NeighborIndices;
import io.nosqlbench.nbvectors.spec.access.datasets.QueryVectors;

public class SpecAccessor {
  private final HdfFile file;

  public SpecAccessor(HdfFile file) {
    this.file = file;
  }

  BaseVectors getBaseVectors() {
    return new BaseVectors(file.getDatasetByPath(SpecDatasets.base_vectors.name()));
  }

  QueryVectors getQueryVectors() {
    return new QueryVectors(file.getDatasetByPath(SpecDatasets.query_vectors.name()));
  }

  NeighborIndices getNeighborIndices() {
    return new NeighborIndices(file.getDatasetByPath(SpecDatasets.neighbor_indices.name()));
  }

  NeighborDistances getNeighborDistances() {
    return new NeighborDistances(file.getDatasetByPath(SpecDatasets.neighbor_distances.name()));
  }
}
