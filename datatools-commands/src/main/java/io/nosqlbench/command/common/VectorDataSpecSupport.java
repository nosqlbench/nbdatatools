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
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetProfileSpec;
import io.nosqlbench.vectordata.spec.datasets.types.DatasetView;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/// Shared helpers for resolving VectorDataSpec facets through the vectordata APIs.
public final class VectorDataSpecSupport {

    private VectorDataSpecSupport() {
    }

    public static boolean requiresRemote(VectorDataSpec spec) {
        return spec.isCatalogFacet() || spec.isRemote();
    }

    public static Path expandPath(Path path) {
        if (path == null) {
            return null;
        }
        String expanded = path.toString()
            .replace("~", System.getProperty("user.home"))
            .replace("${HOME}", System.getProperty("user.home"));
        return Path.of(expanded);
    }

    public static Path requireCacheDir(Path cacheDir) {
        Path resolved = expandPath(cacheDir);
        if (resolved != null) {
            return resolved;
        }
        return VectorDataSettings.load().getCacheDirectory();
    }

    public static TestDataView resolveFacetView(VectorDataSpec spec,
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

    public static Optional<DatasetView<?>> resolveDatasetView(TestDataView view, TestDataKind facetKind) {
        return switch (facetKind) {
            case base_vectors -> view.getBaseVectors().map(v -> (DatasetView<?>) v);
            case query_vectors -> view.getQueryVectors().map(v -> (DatasetView<?>) v);
            case neighbor_indices -> view.getNeighborIndices().map(v -> (DatasetView<?>) v);
            case neighbor_distances -> view.getNeighborDistances().map(v -> (DatasetView<?>) v);
            default -> Optional.empty();
        };
    }

    public static Optional<DatasetView<?>> resolveDatasetView(VectorDataSpec spec,
                                                              Path configDir,
                                                              List<String> catalogs,
                                                              Path cacheDir) {
        if (!spec.isFacet()) {
            return Optional.empty();
        }
        TestDataKind kind = spec.getFacetKind().orElseThrow();
        TestDataView view = resolveFacetView(spec, configDir, catalogs, cacheDir);
        return resolveDatasetView(view, kind);
    }
}
