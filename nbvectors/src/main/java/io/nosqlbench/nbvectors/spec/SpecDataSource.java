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


import io.nosqlbench.nbvectors.commands.build_hdf5.predicates.types.PNode;
import io.nosqlbench.nbvectors.spec.attributes.RootGroupAttributes;
import io.nosqlbench.nbvectors.commands.verify_knn.datatypes.LongIndexedFloatVector;

import java.util.Iterator;
import java.util.Optional;

/// Defines the provider interface for test data which may be used
/// to construct a valid HDF5 test data file.
///
/// Methods which return optional correspond to optional components of the
/// test data spec. Other methods are required to be implemented and return
/// a non-null value.
public interface SpecDataSource {

  /// /base_vectors
  /// @return an interator of base vectors
  /// The index and vector component types are fixed in this version.
  public Iterator<LongIndexedFloatVector> getBaseVectors();

  /// /base_content
  /// @return an iterator of base content
  public Optional<Iterator<?>> getBaseContent();

  /// /query_vectors
  /// @return an iterator of query vectors
  public Iterator<LongIndexedFloatVector> getQueryVectors();

  /// /query_terms
  /// @return an optional iterator of query terms
  public Optional<Iterator<?>> getQueryTerms();

  /// /query_filters
  /// @return an optional iterator of query filters
  public Optional<Iterator<PNode<?>>> getQueryFilters();

  /// /neighbors
  /// @return an iterator of neighbor indices
  public Iterator<int[]> getNeighborIndices();

  /// /distances
  /// @return an iterator of neighbor distances
  public Iterator<float[]> getNeighborDistances();

  /// get metadata
  /// @return the metadata for the test data
  public RootGroupAttributes getMetadata();
}
