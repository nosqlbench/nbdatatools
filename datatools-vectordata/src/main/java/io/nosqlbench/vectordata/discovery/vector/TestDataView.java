package io.nosqlbench.vectordata.discovery.vector;

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


import io.nosqlbench.vectordata.discovery.metadata.MetadataContent;
import io.nosqlbench.vectordata.discovery.metadata.MetadataLayout;
import io.nosqlbench.vectordata.discovery.metadata.PredicateTestDataView;
import io.nosqlbench.vectordata.discovery.metadata.Predicates;
import io.nosqlbench.vectordata.discovery.metadata.ResultIndices;
import io.nosqlbench.vectordata.spec.predicates.PNode;

import java.util.Optional;

/// Combined view interface providing access to both vector and predicate facets
/// of a test dataset profile.
///
/// This is the primary interface callers should use when working with dataset
/// profiles. It extends {@link VectorTestDataView} for vector data (base vectors,
/// query vectors, neighbor indices/distances) and {@link PredicateTestDataView}
/// for predicate data (metadata predicates, result indices, metadata layout/content).
///
/// Predicate methods have default implementations returning {@link Optional#empty()},
/// so datasets that only contain vector data work transparently without requiring
/// predicate support.
public interface TestDataView extends VectorTestDataView, PredicateTestDataView<PNode<?>> {

    /// {@inheritDoc}
    ///
    /// Default implementation returns empty, indicating no predicates are available.
    @Override
    default Optional<Predicates<PNode<?>>> getPredicatesView() {
        return Optional.empty();
    }

    /// {@inheritDoc}
    ///
    /// Default implementation returns empty, indicating no result indices are available.
    @Override
    default Optional<ResultIndices> getResultIndices() {
        return Optional.empty();
    }

    /// {@inheritDoc}
    ///
    /// Default implementation returns empty, indicating no metadata layout is available.
    @Override
    default Optional<MetadataLayout> getMetadataLayout() {
        return Optional.empty();
    }

    /// {@inheritDoc}
    ///
    /// Default implementation returns empty, indicating no metadata content is available.
    @Override
    default Optional<MetadataContent> getMetadataContentView() {
        return Optional.empty();
    }
}
