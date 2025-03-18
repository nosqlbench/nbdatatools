package io.nosqlbench.nbvectors.commands.build_hdf5;

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
import io.nosqlbench.nbvectors.commands.build_hdf5.predicates.types.PNode;
import io.nosqlbench.nbvectors.commands.export_hdf5.FvecToFloatArray;
import io.nosqlbench.nbvectors.commands.export_hdf5.FvecToIndexedFloatVector;
import io.nosqlbench.nbvectors.commands.export_hdf5.IvecToIntArray;
import io.nosqlbench.nbvectors.spec.attributes.RootGroupAttributes;
import io.nosqlbench.nbvectors.spec.SpecDataSource;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;

import java.util.Iterator;
import java.util.Optional;

/// A basic test data source which provides iterators for all the required data,
/// and allows for setting the iterators for each component as needed.
public class BasicTestDataSource implements SpecDataSource {

  private Iterator<LongIndexedFloatVector> baseVectorsIterator;
  private Iterator<?> baseContentIter;
  private Iterator<LongIndexedFloatVector> queryVectorsIter;
  private Iterator<?> queryTermsIter;
  private Iterator<PNode<?>> queryFiltersIter;
  private Iterator<int[]> neighborIndicesIter;
  private RootGroupAttributes metadata;
  private Iterator<float[]> neighborDistancesIter;


  /// create a new basic test data source
  public BasicTestDataSource() {
  }

  /// create a new basic test data source from a file config
  /// @param cfg
  ///     the file config
  public BasicTestDataSource(VectorFilesConfig cfg) {
    setBaseVectorsIterator(new FvecToIndexedFloatVector(cfg.base_vectors()).iterator());
    setQueryVectorsIter(new FvecToIndexedFloatVector(cfg.query_vectors()).iterator());
    setNeighborIndicesIter(new IvecToIntArray(cfg.neighbors()).iterator());
    setNeighborDistancesIter(new FvecToFloatArray(cfg.distances()).iterator());
    setMetadata(cfg.metadata());
  }

  @Override
  public Iterator<LongIndexedFloatVector> getBaseVectors() {
    return this.baseVectorsIterator;
  }

  @Override
  public Optional<Iterator<?>> getBaseContent() {
    return Optional.ofNullable(this.baseContentIter);
  }

  @Override
  public Iterator<LongIndexedFloatVector> getQueryVectors() {
    return this.queryVectorsIter;
  }

  @Override
  public Optional<Iterator<?>> getQueryTerms() {
    return Optional.ofNullable(this.queryTermsIter);
  }

  @Override
  public Optional<Iterator<PNode<?>>> getQueryFilters() {
    return Optional.ofNullable(this.queryFiltersIter);
  }

  @Override
  public Iterator<int[]> getNeighborIndices() {
    return this.neighborIndicesIter;
  }

  @Override
  public Optional<Iterator<float[]>> getNeighborDistances() {
    return Optional.ofNullable(this.neighborDistancesIter);
  }

  @Override
  public RootGroupAttributes getMetadata() {
    return this.metadata;
  }

  /// Set the iterator for the base vectors
  /// @param baseVectorsIterator
  ///     the iterator for the base vectors
  public void setBaseVectorsIterator(Iterator<LongIndexedFloatVector> baseVectorsIterator) {
    this.baseVectorsIterator = baseVectorsIterator;
  }

  /// Set the iterator for the base content
  /// @param baseContentIter
  ///     the iterator for the base content
  public void setBaseContentIter(Iterator<?> baseContentIter) {
    this.baseContentIter = baseContentIter;
  }

  /// Set the iterator for the query vectors
  /// @param queryVectorsIter
  ///     the iterator for the query vectors
  public void setQueryVectorsIter(Iterator<LongIndexedFloatVector> queryVectorsIter) {
    this.queryVectorsIter = queryVectorsIter;
  }

  /// Set the iterator for the query terms
  /// @param queryTermsIter
  ///     the iterator for the query terms
  public void setQueryTermsIter(Iterator<?> queryTermsIter) {
    this.queryTermsIter = queryTermsIter;
  }

  /// Set the iterator for the query filters
  /// @param queryFiltersIter
  ///     the iterator for the query filters
  public void setQueryFiltersIter(Iterator<PNode<?>> queryFiltersIter) {
    this.queryFiltersIter = queryFiltersIter;
  }

  /// Set the iterator for the neighbor indices
  /// @param neighborIndicesIter
  ///     the iterator for the neighbor indices
  public void setNeighborIndicesIter(Iterator<int[]> neighborIndicesIter) {
    this.neighborIndicesIter = neighborIndicesIter;
  }

  /// Set the iterator for the neighbor distances
  /// @param neighborDistancesIter
  ///     the iterator for the neighbor distances
  public void setNeighborDistancesIter(Iterator<float[]> neighborDistancesIter) {
    this.neighborDistancesIter = neighborDistancesIter;
  }

  /// Set the metadata
  /// @param metadata
  ///     the metadata
  public void setMetadata(RootGroupAttributes metadata) {
    this.metadata = metadata;
  }

}
