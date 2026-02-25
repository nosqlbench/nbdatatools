package io.nosqlbench.vectordata.discovery.metadata;

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


import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/// This is the entry to consuming Vector Test Data from a profile according to the documented spec.
///
/// TestDataView provides access to vector datasets and their associated metadata, including
/// base vectors, query vectors, neighbor indices, and distance metrics. It also provides
/// methods for accessing metadata like license, vendor, and model information.
public interface PredicateTestDataView<T> {
  /// Get the base vectors dataset
  /// @return the base vectors dataset
  Optional<Predicates<T>> getPredicatesView();

  /// Get the result indices dataset, the ordinals matching the metadata records for an associated predicate
  /// @return the result indices dataset
  Optional<ResultIndices> getResultIndices();

  ///  Get the description of the metadata, the field names, the field types, and value descriptions
  ///  @return the metadata layout
  Optional<MetadataLayout> getMetadataLayout();


  /// Get the actual metadata which constitutes a row in a database or an object in an object store
  /// @return the metadata content view
  Optional<MetadataContent> getMetadataContentView();

  /// Prebuffer all datasets in this test data view
  /// @return a future that completes when all prebuffering is done
  CompletableFuture<Void> prebuffer();

}
