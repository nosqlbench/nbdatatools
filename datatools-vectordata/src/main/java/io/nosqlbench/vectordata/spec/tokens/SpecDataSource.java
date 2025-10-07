package io.nosqlbench.vectordata.spec.tokens;

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


import io.nosqlbench.vectordata.spec.attributes.RootGroupAttributes;
import io.nosqlbench.vectordata.spec.predicates.PNode;

import java.util.Optional;

/// Defines the provider interface for test data which may be used
/// to construct a valid HDF5 test data file.
///
/// Methods which return optional correspond to optional components of the
/// test data spec. Other methods are required to be implemented and return
/// a non-null value.
public interface SpecDataSource {

  /// /base_vectors
  /// @return an iterator of base vectors
  /// The index and vector component types are fixed in this version.
  public Optional<Iterable<float[]>> getBaseVectors();

  /// /base_content
  /// @return an iterator of base content
  public Optional<Iterable<?>> getBaseContent();

  /// /query_vectors
  /// @return an iterator of query vectors
  public Optional<Iterable<float[]>> getQueryVectors();

  /// /query_terms
  /// @return an optional iterator of query terms
  public Optional<Iterable<?>> getQueryTerms();

  /// /query_filters
  /// @return an optional iterator of query filters
  public Optional<Iterable<PNode<?>>> getQueryFilters();

  /// /neighbors
  /// @return an iterator of neighbor indices
  public Optional<Iterable<int[]>> getNeighborIndices();

  /// /distances
  /// @return an iterator of neighbor distances
  public Optional<Iterable<float[]>> getNeighborDistances();

  /// get metadata
  /// @return the metadata for the test data
  public RootGroupAttributes getMetadata();
}
