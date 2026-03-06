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
import io.nosqlbench.vectordata.spec.datasets.types.DatasetView;
import io.nosqlbench.vectordata.spec.datasets.types.FacetDescriptor;
import io.nosqlbench.vectordata.spec.predicates.PNode;

import java.util.Collections;
import java.util.Map;
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
///
/// ## Custom facets
///
/// Dataset profiles in {@code dataset.yaml} may declare facets beyond the core
/// set (base_vectors, query_vectors, neighbor_indices, etc.). These non-core
/// facets are not accessible through the typed methods above, but they are
/// still part of the profile and can be discovered and loaded at runtime.
///
/// Two methods provide this capability:
///
/// - {@link #getFacetManifest()} — returns a map of **all** facet names (core
///   and non-core) to lightweight {@link FacetDescriptor} objects. Descriptors
///   carry metadata (source path, source type, whether the facet is a standard
///   {@link io.nosqlbench.vectordata.spec.datasets.types.TestDataKind}) without
///   materializing the underlying data. Use this to discover what facets a
///   profile provides before deciding which ones to load.
///
/// - {@link #getFacet(String)} — materializes and returns the {@link DatasetView}
///   for a named facet. This is the general-purpose catch-all: it works for both
///   core facets (as an alternative to the typed accessors) and non-core facets
///   that have no dedicated accessor. The caller should cast the returned
///   {@link DatasetView} to the concrete type appropriate for the facet.
///
/// ### Example: loading a custom facet
///
/// ```java
/// TestDataView view = ...;
/// Map<String, FacetDescriptor> manifest = view.getFacetManifest();
/// FacetDescriptor desc = manifest.get("my_custom_embeddings");
/// if (desc != null && !desc.isStandard()) {
///     DatasetView<?> custom = view.getFacet("my_custom_embeddings").orElseThrow();
///     // cast to the expected type based on your knowledge of the dataset
/// }
/// ```
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

    /// Returns lightweight descriptors for all facets declared in this profile,
    /// without materializing the underlying data.
    ///
    /// The returned map includes both core facets (those matching a
    /// {@link io.nosqlbench.vectordata.spec.datasets.types.TestDataKind} value,
    /// such as {@code base_vectors} or {@code neighbor_indices}) and non-core
    /// facets whose names are specific to the dataset. Each
    /// {@link FacetDescriptor} carries the facet's source path, source type,
    /// and whether it maps to a standard {@code TestDataKind}.
    ///
    /// This is the discovery step: inspect the manifest first, then call
    /// {@link #getFacet(String)} only for the facets you actually need.
    ///
    /// @return an unmodifiable map of facet names to their descriptors
    default Map<String, FacetDescriptor> getFacetManifest() {
        return Collections.emptyMap();
    }

    /// Materializes and returns the {@link DatasetView} for a named facet.
    ///
    /// This is the general-purpose catch-all for loading any facet by name,
    /// whether it is a core facet with a dedicated typed accessor (e.g.,
    /// {@code base_vectors} via {@link VectorTestDataView#getBaseVectors()})
    /// or a non-core facet that only appears in the dataset's
    /// {@code dataset.yaml} profile. Non-core facets have no dedicated
    /// accessor on this interface — this method is the only way to load them.
    ///
    /// The returned {@link DatasetView} is parameterized with a wildcard.
    /// Callers who know the concrete element type of a specific facet should
    /// cast the result accordingly.
    ///
    /// @param name the facet name as declared in dataset.yaml (e.g.,
    ///             {@code "base_vectors"} or {@code "my_custom_embeddings"})
    /// @return the materialized dataset view, or empty if no facet with
    ///         that name exists in this profile
    Optional<DatasetView<?>> getFacet(String name);
}
