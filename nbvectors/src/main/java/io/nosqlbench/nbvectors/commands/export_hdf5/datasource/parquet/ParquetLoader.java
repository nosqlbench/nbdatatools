package io.nosqlbench.nbvectors.commands.export_hdf5.datasource.parquet;

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


import io.nosqlbench.nbvectors.commands.build_hdf5.predicates.types.PNode;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.spec.access.datasets.types.BaseVectors;
import io.nosqlbench.nbvectors.spec.attributes.RootGroupAttributes;
import io.nosqlbench.nbvectors.spec.views.SpecDataSource;

import java.util.Optional;

public class ParquetLoader implements SpecDataSource {

  private final ParquetLoaderConfig config;

  public ParquetLoader(ParquetLoaderConfig config) {
    this.config = config;
  }

  @Override
  public Optional<Iterable<float[]>> getBaseVectors() {
    return config.getBaseVectorsLayout().map(ParquetVectorsReader::new);
  }

  @Override
  public Optional<Iterable<?>> getBaseContent() {
    return Optional.empty();
  }

  @Override
  public Optional<Iterable<LongIndexedFloatVector>> getQueryVectors() {
    return Optional.empty();
  }

  @Override
  public Optional<Iterable<?>> getQueryTerms() {
    return Optional.empty();
  }

  @Override
  public Optional<Iterable<PNode<?>>> getQueryFilters() {
    return Optional.empty();
  }

  @Override
  public Optional<Iterable<int[]>> getNeighborIndices() {
    return Optional.empty();
  }

  @Override
  public Optional<Iterable<float[]>> getNeighborDistances() {
    return Optional.empty();
  }

  @Override
  public RootGroupAttributes getMetadata() {
    return null;
  }
}
