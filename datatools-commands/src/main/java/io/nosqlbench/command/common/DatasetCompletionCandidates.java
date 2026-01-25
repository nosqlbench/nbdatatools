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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.command.common;

import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetEntry;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/// Provides dynamic completion candidates for dataset specifications.
///
/// This class generates completion candidates for dataset names, profiles, and facets
/// by querying the configured catalogs. It supports the following formats:
/// - `datasetname` - just the dataset name
/// - `datasetname:profilename` - dataset with profile
/// - `datasetname:profilename:facetname` - dataset with profile and facet
///
/// For shell completion to work, users must first generate the completion script:
/// ```bash
/// source <(nbvectors generate-completion)
/// ```
public class DatasetCompletionCandidates implements Iterable<String> {

    private static final Path DEFAULT_CONFIG_DIR = Path.of(
        System.getProperty("user.home"), ".config", "vectordata");

    /// Standard facet names for completion
    private static final List<String> FACET_NAMES = List.of(
        "base", "query", "indices", "distances",
        "base_vectors", "query_vectors", "neighbor_indices", "neighbor_distances"
    );

    @Override
    public Iterator<String> iterator() {
        List<String> candidates = new ArrayList<>();

        try {
            TestDataSources config = new TestDataSources().configure(DEFAULT_CONFIG_DIR);
            Catalog catalog = Catalog.of(config);

            // Add dataset names
            for (DatasetEntry entry : catalog.datasets()) {
                String datasetName = entry.name();
                candidates.add(datasetName);

                // Add dataset:profile combinations
                if (entry.profiles() != null) {
                    for (String profileName : entry.profiles().keySet()) {
                        String datasetProfile = datasetName + ":" + profileName;
                        candidates.add(datasetProfile);

                        // Add dataset:profile:facet combinations
                        for (String facet : FACET_NAMES) {
                            candidates.add(datasetProfile + ":" + facet);
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Silently fail - completion is best-effort
            // Return empty list if catalog cannot be loaded
        }

        return candidates.iterator();
    }

    /// Completion candidates for dataset names only (no profiles or facets).
    public static class DatasetOnly implements Iterable<String> {
        @Override
        public Iterator<String> iterator() {
            List<String> candidates = new ArrayList<>();

            try {
                TestDataSources config = new TestDataSources().configure(DEFAULT_CONFIG_DIR);
                Catalog catalog = Catalog.of(config);

                for (DatasetEntry entry : catalog.datasets()) {
                    candidates.add(entry.name());
                }
            } catch (Exception e) {
                // Silently fail
            }

            return candidates.iterator();
        }
    }

    /// Completion candidates for dataset:profile format.
    public static class DatasetProfile implements Iterable<String> {
        @Override
        public Iterator<String> iterator() {
            List<String> candidates = new ArrayList<>();

            try {
                TestDataSources config = new TestDataSources().configure(DEFAULT_CONFIG_DIR);
                Catalog catalog = Catalog.of(config);

                for (DatasetEntry entry : catalog.datasets()) {
                    String datasetName = entry.name();

                    if (entry.profiles() != null) {
                        for (String profileName : entry.profiles().keySet()) {
                            String datasetProfile = datasetName + ":" + profileName;

                            for (String facet : FACET_NAMES) {
                                candidates.add(datasetProfile + ":" + facet);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                // Silently fail
            }

            return candidates.iterator();
        }
    }

    /// Completion candidates for dataset:profile:facet format.
    public static class DatasetProfileFacet implements Iterable<String> {
        @Override
        public Iterator<String> iterator() {
            List<String> candidates = new ArrayList<>();

            try {
                TestDataSources config = new TestDataSources().configure(DEFAULT_CONFIG_DIR);
                Catalog catalog = Catalog.of(config);

                for (DatasetEntry entry : catalog.datasets()) {
                    String datasetName = entry.name();

                    if (entry.profiles() != null) {
                        for (String profileName : entry.profiles().keySet()) {
                            candidates.add(datasetName + ":" + profileName);
                        }
                    }
                }
            } catch (Exception e) {
                // Silently fail
            }

            return candidates.iterator();
        }
    }

    /// Completion candidates for facet names only.
    public static class FacetOnly implements Iterable<String> {
        @Override
        public Iterator<String> iterator() {
            return FACET_NAMES.iterator();
        }
    }
}
