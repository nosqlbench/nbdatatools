package io.nosqlbench.nbvectors.commands.build_hdf5.datasource;

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


import io.nosqlbench.nbvectors.commands.export_hdf5.VectorFilesConfig;
import io.nosqlbench.vectordata.internalapi.predicates.PNode;
import io.nosqlbench.nbvectors.commands.export_hdf5.datasource.ivecfvec.FvecToFloatArray;
import io.nosqlbench.nbvectors.common.adapters.DataSourceAdapter;
import io.nosqlbench.vectordata.internalapi.datasets.attrs.RootGroupAttributes;
import io.nosqlbench.vectordata.internalapi.tokens.SpecDataSource;

import java.util.Optional;

/// A basic test data source which provides iterators for all the required data,
/// and allows for setting the iterators for each component as needed.
public class BasicTestDataSource implements SpecDataSource {

  private Iterable<float[]> baseVectorsIterable;
  private RootGroupAttributes metadata;
  private Iterable<float[]> queryVectorsIterable;
  private Iterable<?> queryTermsIterable;
  private Iterable<PNode<?>> queryFiltersIterable;
  private Iterable<int[]> neighborIndicesIterable;
  private Iterable<float[]> neighborDistancesIterable;
  private Iterable<?> baseContentIterable;


  /// create a new basic test data source
  public BasicTestDataSource() {
  }

  /// create a new basic test data source from a file config
  /// @param cfg
  ///     the file config
  public BasicTestDataSource(VectorFilesConfig cfg) {

    cfg.base_content().map(DataSourceAdapter::adaptBaseContent)
        .ifPresent(this::setBaseContentIterable);
    cfg.base_vectors().map(DataSourceAdapter::adaptBaseVectors)
        .ifPresent(this::setBaseVectorsIterable);
    cfg.query_vectors().map(DataSourceAdapter::adaptQueryVectors)
        .ifPresent(this::setQueryVectorsIterable);
    cfg.neighbors().map(DataSourceAdapter::adaptNeighborIndices)
        .ifPresent(this::setNeighborIndicesIterable);
    cfg.distances().map(DataSourceAdapter::adaptNeighborDistances)
        .ifPresent(this::setNeighborDistancesIter);
    setMetadata(cfg.metadata());
  }

  /// Set the iterable for the neighbor distances
  /// @param neighborDistancesIterable
  ///     the iterable for the neighbor distances
  /// @see FvecToFloatArray
  public void setNeighborDistancesIterable(FvecToFloatArray neighborDistancesIterable) {
    this.neighborDistancesIterable = neighborDistancesIterable;
  }

  @Override
  public Optional<Iterable<float[]>> getBaseVectors() {
    return Optional.ofNullable(baseVectorsIterable);
  }

  @Override
  public Optional<Iterable<?>> getBaseContent() {
    return Optional.ofNullable(this.baseContentIterable);
  }

  @Override
  public Optional<Iterable<float[]>> getQueryVectors() {
    return Optional.ofNullable(this.queryVectorsIterable);
  }

  @Override
  public Optional<Iterable<?>> getQueryTerms() {
    return Optional.ofNullable(this.queryTermsIterable);
  }

  @Override
  public Optional<Iterable<PNode<?>>> getQueryFilters() {
    return Optional.ofNullable(this.queryFiltersIterable);
  }

  @Override
  public Optional<Iterable<int[]>> getNeighborIndices() {
    return Optional.ofNullable(this.neighborIndicesIterable);
  }

  @Override
  public Optional<Iterable<float[]>> getNeighborDistances() {
    return Optional.ofNullable(this.neighborDistancesIterable);
  }

  @Override
  public RootGroupAttributes getMetadata() {
    return this.metadata;
  }

  /// Set the iterator for the base vectors
  /// @param baseVectorsIterable
  ///     the iterable for the base vectors
  public void setBaseVectorsIterable(Iterable<float[]> baseVectorsIterable) {
    this.baseVectorsIterable = baseVectorsIterable;
  }

  /// Set the iterator for the base content
  /// @param baseContentIterable
  ///     the iterator for the base content
  public void setBaseContentIterable(Iterable<?> baseContentIterable) {
    this.baseContentIterable = baseContentIterable;
  }

  /// Set the iterator for the query vectors
  /// @param queryVectorsIterable
  ///     the iterator for the query vectors
  public void setQueryVectorsIterable(Iterable<float[]> queryVectorsIterable) {
    this.queryVectorsIterable = queryVectorsIterable;
  }

  /// Set the iterable for the query terms
  /// @param queryTermsIterable
  ///     the iterable for the query terms
  public void setQueryTermsIterable(Iterable<?> queryTermsIterable) {
    this.queryTermsIterable = queryTermsIterable;
  }

  /// Set the iterator for the query filters
  /// @param queryFiltersIter
  ///     the iterator for the query filters
  public void setQueryFiltersIter(Iterable<PNode<?>> queryFiltersIter) {
    this.queryFiltersIterable = queryFiltersIter;
  }

  /// Set the iterable for the neighbor indices
  /// @param neighborIndicesIterable
  ///     the iterable for the neighbor indices
  public void setNeighborIndicesIterable(Iterable<int[]> neighborIndicesIterable) {
    this.neighborIndicesIterable = neighborIndicesIterable;
  }

  /// Set the iteraable for the neighbor distances
  /// @param neighborDistancesIterable
  ///     the iterable for the neighbor distances
  public void setNeighborDistancesIter(Iterable<float[]> neighborDistancesIterable) {
    this.neighborDistancesIterable = neighborDistancesIterable;
  }

  /// Set the metadata
  /// @param metadata
  ///     the metadata
  public void setMetadata(RootGroupAttributes metadata) {
    this.metadata = metadata;
  }

}
