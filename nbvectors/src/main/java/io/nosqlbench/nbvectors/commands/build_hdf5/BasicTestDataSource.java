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


import io.nosqlbench.nbvectors.commands.build_hdf5.predicates.types.PNode;
import io.nosqlbench.nbvectors.spec.attributes.SpecAttributes;
import io.nosqlbench.nbvectors.spec.SpecDataSource;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;

import java.util.Iterator;
import java.util.Optional;

public class BasicTestDataSource implements SpecDataSource {

  private Iterator<LongIndexedFloatVector> baseVectorsIterator;
  private Iterator<?> baseContentIter;
  private Iterator<LongIndexedFloatVector> queryVectorsIter;
  private Iterator<?> queryTermsIter;
  private Iterator<PNode<?>> queryFiltersIter;
  private Iterator<int[]> neighborIndicesIter;
  private SpecAttributes metadata;
  private Iterator<float[]> neighborDistancesIter;

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
  public Iterator<float[]> getNeighborDistances() {
    return this.neighborDistancesIter;
  }

  @Override
  public SpecAttributes getMetadata() {
    return this.metadata;
  }

  public void setBaseVectorsIterator(Iterator<LongIndexedFloatVector> baseVectorsIterator) {
    this.baseVectorsIterator = baseVectorsIterator;
  }

  /// Set the iterator for the base content
  /// @param baseContentIter the iterator for the base content
  public void setBaseContentIter(Iterator<?> baseContentIter) {
    this.baseContentIter = baseContentIter;
  }

  /// Set the iterator for the query vectors
  /// @param queryVectorsIter the iterator for the query vectors
  public void setQueryVectorsIter(Iterator<LongIndexedFloatVector> queryVectorsIter) {
    this.queryVectorsIter = queryVectorsIter;
  }

  /// Set the iterator for the query terms
  /// @param queryTermsIter the iterator for the query terms
  public void setQueryTermsIter(Iterator<?> queryTermsIter) {
    this.queryTermsIter = queryTermsIter;
  }

  /// Set the iterator for the query filters
  /// @param queryFiltersIter the iterator for the query filters
  public void setQueryFiltersIter(Iterator<PNode<?>> queryFiltersIter) {
    this.queryFiltersIter = queryFiltersIter;
  }

  /// Set the iterator for the neighbor indices
  /// @param neighborIndicesIter the iterator for the neighbor indices
  public void setNeighborIndicesIter(Iterator<int[]> neighborIndicesIter) {
    this.neighborIndicesIter = neighborIndicesIter;
  }

  /// Set the iterator for the neighbor distances
  /// @param neighborDistancesIter the iterator for the neighbor distances
  public void setNeighborDistancesIter(Iterator<float[]> neighborDistancesIter) {
    this.neighborDistancesIter = neighborDistancesIter;
  }

  /// Set the metadata
  /// @param metadata the metadata
  public void setMetadata(SpecAttributes metadata) {
    this.metadata = metadata;
  }
}
