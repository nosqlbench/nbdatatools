package io.nosqlbench.vectordata.spec.datasets.types;

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

/// Lightweight descriptor for a dataset facet, providing metadata about the
/// facet without requiring materialization of the underlying data.
///
/// This is returned by {@link io.nosqlbench.vectordata.discovery.vector.TestDataView#getFacetManifest()}
/// to allow callers to inspect available facets before deciding which ones
/// to load via {@link io.nosqlbench.vectordata.discovery.vector.TestDataView#getFacet(String)}.
///
/// ## Standard vs custom facets
///
/// Facets whose names match a {@link TestDataKind} value are standard facets
/// and are also accessible through the typed accessors on {@code TestDataView}
/// (e.g., {@code getBaseVectors()}, {@code getQueryVectors()}). Custom facets
/// are only accessible through the {@code getFacet(String)} escape hatch.
public final class FacetDescriptor {

    private final String name;
    private final String sourcePath;
    private final String sourceType;
    private final TestDataKind standardKind;

    /// Creates a facet descriptor.
    ///
    /// @param name         the facet name as declared in dataset.yaml
    /// @param sourcePath   the source file path, or null if not available
    /// @param sourceType   the source type name (e.g., "xvec", "slab", "virtdata"), or null
    /// @param standardKind the matching TestDataKind if this is a standard facet, or null
    public FacetDescriptor(String name, String sourcePath, String sourceType, TestDataKind standardKind) {
        this.name = name;
        this.sourcePath = sourcePath;
        this.sourceType = sourceType;
        this.standardKind = standardKind;
    }

    /// Returns the facet name as declared in dataset.yaml.
    /// @return the facet name
    public String name() {
        return name;
    }

    /// Returns the source file path for this facet, if available.
    /// @return the source path, or null
    public String sourcePath() {
        return sourcePath;
    }

    /// Returns the source type name (e.g., "xvec", "slab", "virtdata"), if available.
    /// @return the source type, or null
    public String sourceType() {
        return sourceType;
    }

    /// Returns the matching standard {@link TestDataKind}, if this facet corresponds
    /// to one of the enumerated kinds. Empty for custom facets.
    /// @return the standard kind, or empty for custom facets
    public Optional<TestDataKind> standardKind() {
        return Optional.ofNullable(standardKind);
    }

    /// Returns whether this is a standard facet (matching a {@link TestDataKind} value).
    /// @return true if standard, false if custom
    public boolean isStandard() {
        return standardKind != null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("FacetDescriptor{name='").append(name).append('\'');
        if (sourcePath != null) sb.append(", source='").append(sourcePath).append('\'');
        if (sourceType != null) sb.append(", type=").append(sourceType);
        if (standardKind != null) sb.append(", kind=").append(standardKind);
        sb.append('}');
        return sb.toString();
    }
}
