/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.nosqlbench.command.common;

import io.nosqlbench.vectordata.config.VectorDataSettings;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.discovery.vector.VectorTestDataView;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetProfileSpec;
import io.nosqlbench.vectordata.spec.datasets.types.VectorDatasetView;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/// Shared helpers for resolving VectorDataSpec facets through the vectordata APIs.
public final class VectorDataSpecSupport {

    private VectorDataSpecSupport() {
    }

    /// Checks if the given spec requires remote access.
    /// @param spec the vector data specification
    /// @return true if the spec requires remote resources
    public static boolean requiresRemote(VectorDataSpec spec) {
        return spec.isCatalogFacet() || spec.isRemote();
    }

    /// Expands home directory variables in the given path.
    /// @param path the path to expand
    /// @return the expanded path, or null if input was null
    public static Path expandPath(Path path) {
        if (path == null) {
            return null;
        }
        String expanded = path.toString()
            .replace("~", System.getProperty("user.home"))
            .replace("${HOME}", System.getProperty("user.home"));
        return Path.of(expanded);
    }

    /// Resolves the cache directory, falling back to the configured default.
    /// @param cacheDir the explicit cache directory, or null
    /// @return the resolved cache directory
    public static Path requireCacheDir(Path cacheDir) {
        Path resolved = expandPath(cacheDir);
        if (resolved != null) {
            return resolved;
        }
        return VectorDataSettings.load().getCacheDirectory();
    }

    /// Resolves a facet spec into a VectorTestDataView.
    /// @param spec the facet specification
    /// @param configDir the configuration directory
    /// @param catalogs additional catalog paths
    /// @param cacheDir the cache directory
    /// @return the resolved vector test data view
    public static VectorTestDataView resolveFacetView(VectorDataSpec spec,
                                                      Path configDir,
                                                      List<String> catalogs,
                                                      Path cacheDir) {
        if (!spec.isFacet()) {
            throw new IllegalArgumentException("Spec is not a facet: " + spec);
        }

        String profileName = spec.getProfileName().orElseThrow();
        TestDataSources config = new TestDataSources().configure(expandPath(configDir));

        Catalog catalog;
        String datasetName;
        if (spec.isLocalFacet()) {
            Path datasetDir = spec.getLocalPath().orElseThrow();
            config = config.addCatalogs(List.of(datasetDir.toString()));
            catalog = Catalog.of(config);
            datasetName = datasetDir.getFileName().toString();
        } else {
            if (catalogs != null && !catalogs.isEmpty()) {
                config = config.addCatalogs(catalogs);
            }
            catalog = Catalog.of(config);
            datasetName = spec.getDatasetRef().orElseThrow();
        }

        DatasetProfileSpec datasetSpec = DatasetProfileSpec.parse(datasetName + ":" + profileName);
        ProfileSelector selector = catalog.select(datasetSpec);
        Path resolvedCacheDir = requireCacheDir(cacheDir);
        selector = selector.setCacheDir(resolvedCacheDir.toString());
        return selector.profile(profileName);
    }

    /// Resolves a dataset view for the given facet kind.
    /// @param view the vector test data view
    /// @param facetKind the kind of facet to resolve
    /// @return the resolved dataset view, or empty
    public static Optional<VectorDatasetView<?>> resolveDatasetView(VectorTestDataView view, TestDataKind facetKind) {
        return switch (facetKind) {
            case base_vectors -> view.getBaseVectors().map(v -> (VectorDatasetView<?>) v);
            case query_vectors -> view.getQueryVectors().map(v -> (VectorDatasetView<?>) v);
            case neighbor_indices -> view.getNeighborIndices().map(v -> (VectorDatasetView<?>) v);
            case neighbor_distances -> view.getNeighborDistances().map(v -> (VectorDatasetView<?>) v);
            case filtered_neighbor_indices -> view.getFilteredNeighborIndices().map(v -> (VectorDatasetView<?>) v);
            case filtered_neighbor_distances -> view.getFilteredNeighborDistances().map(v -> (VectorDatasetView<?>) v);
            case metadata_predicates, predicate_results, metadata_layout, metadata_content ->
                // Predicate kinds are handled via predicateProfile(), not through VectorTestDataView
                Optional.empty();
            default -> Optional.empty();
        };
    }

    /// Resolves a facet spec directly into a dataset view.
    /// @param spec the facet specification
    /// @param configDir the configuration directory
    /// @param catalogs additional catalog paths
    /// @param cacheDir the cache directory
    /// @return the resolved dataset view, or empty
    public static Optional<VectorDatasetView<?>> resolveDatasetView(VectorDataSpec spec,
                                                                    Path configDir,
                                                                    List<String> catalogs,
                                                                    Path cacheDir) {
        if (!spec.isFacet()) {
            return Optional.empty();
        }
        TestDataKind kind = spec.getFacetKind().orElseThrow();
        VectorTestDataView view = resolveFacetView(spec, configDir, catalogs, cacheDir);
        return resolveDatasetView(view, kind);
    }
}
