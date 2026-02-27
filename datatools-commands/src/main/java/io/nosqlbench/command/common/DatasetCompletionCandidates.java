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
import io.nosqlbench.vectordata.layoutv2.DSProfile;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

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

    /// Creates a new DatasetCompletionCandidates instance.
    public DatasetCompletionCandidates() {
    }

    private static final Path DEFAULT_CONFIG_DIR = Path.of(
        System.getProperty("user.home"), ".config", "vectordata");

    private static final List<String> CANONICAL_FACET_NAMES = canonicalFacetNames();

    @Override
    public Iterator<String> iterator() {
        return new DatasetProfileFacet().iterator();
    }

    /// Completion candidates for dataset names only (no profiles or facets).
    public static class DatasetOnly implements Iterable<String> {

        /// Creates a new DatasetOnly instance.
        public DatasetOnly() {
        }

        @Override
        public Iterator<String> iterator() {
            List<String> candidates = new ArrayList<>();

            try {
                TestDataSources config = new TestDataSources().configure(DEFAULT_CONFIG_DIR);
                Catalog catalog = Catalog.of(config);

                for (DatasetEntry entry : catalog.datasets()) {
                    if (entry.name().contains(".")) {
                        continue;
                    }
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

        /// Creates a new DatasetProfile instance.
        public DatasetProfile() {
        }

        @Override
        public Iterator<String> iterator() {
            List<String> candidates = new ArrayList<>();

            try {
                Optional<CompletionLineParser.ParsedLine> parsed = CompletionLineParser.parseFromEnv();
                String currentArg = parsed.map(CompletionLineParser.ParsedLine::currentArgPrefix).orElse("");
                if (currentArg.startsWith("http://") || currentArg.startsWith("https://")) {
                    return candidates.iterator();
                }

                CompletionCatalog catalog = CompletionCatalog.load(DEFAULT_CONFIG_DIR);
                List<String> datasets = new ArrayList<>(catalog.profilesByDataset().keySet());
                if (datasets.isEmpty()) {
                    return candidates.iterator();
                }

                if (!currentArg.contains(":") && !currentArg.contains(".")) {
                    if (!currentArg.isEmpty()) {
                        Optional<String> uniqueMatch = findUniqueDatasetMatch(currentArg, datasets);
                        if (uniqueMatch.isPresent()) {
                            String datasetName = uniqueMatch.get();
                            List<String> profiles = catalog.profilesByDataset().get(datasetName);
                            if (profiles != null && !profiles.isEmpty()) {
                                candidates.add(datasetName + ".");
                                return candidates.iterator();
                            }
                        }
                    }
                    addDatasetCandidates(candidates, datasets, currentArg);
                    return candidates.iterator();
                }

                String separator = currentArg.contains(".") ? "." : ":";
                String[] parts = currentArg.split(Pattern.quote(separator), -1);
                if (parts.length < 2) {
                    addDatasetCandidates(candidates, datasets, "");
                    return candidates.iterator();
                }

                String datasetName = parts[0];
                List<String> profiles = catalog.profilesByDataset().get(datasetName);
                if (datasetName.isEmpty() || profiles == null || profiles.isEmpty()) {
                    return candidates.iterator();
                }

                if (parts.length > 2) {
                    return candidates.iterator();
                }

                String profilePrefix = parts[1];
                for (String profileName : profiles) {
                    if (profilePrefix.isEmpty() || profileName.startsWith(profilePrefix)) {
                        candidates.add(datasetName + "." + profileName);
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

        /// Creates a new DatasetProfileFacet instance.
        public DatasetProfileFacet() {
        }

        @Override
        public Iterator<String> iterator() {
            List<String> candidates = new ArrayList<>();

            try {
                Optional<CompletionLineParser.ParsedLine> parsed = CompletionLineParser.parseFromEnv();
                String currentArg = parsed.map(CompletionLineParser.ParsedLine::currentArgPrefix).orElse("");
                if (currentArg.startsWith("http://") || currentArg.startsWith("https://")) {
                    return candidates.iterator();
                }

                CompletionCatalog catalog = CompletionCatalog.load(DEFAULT_CONFIG_DIR);
                List<String> datasets = new ArrayList<>(catalog.profilesByDataset().keySet());
                if (datasets.isEmpty()) {
                    return candidates.iterator();
                }

                if (!currentArg.contains(":") && !currentArg.contains(".")) {
                    addDatasetCandidates(candidates, datasets, currentArg);
                    return candidates.iterator();
                }

                String separator = currentArg.contains(".") ? "." : ":";
                String[] parts = currentArg.split(Pattern.quote(separator), -1);
                if (parts.length < 2) {
                    addDatasetCandidates(candidates, datasets, "");
                    return candidates.iterator();
                }

                String datasetName = parts[0];
                List<String> profiles = catalog.profilesByDataset().get(datasetName);
                if (datasetName.isEmpty() || profiles == null || profiles.isEmpty()) {
                    return candidates.iterator();
                }

                if (parts.length == 2) {
                    for (String profileName : profiles) {
                        candidates.add(datasetName + "." + profileName + ".");
                    }
                    return candidates.iterator();
                }

                String profileName = parts[1];
                if (profileName.isEmpty() || !profiles.contains(profileName)) {
                    return candidates.iterator();
                }

                List<String> facets = catalog.facetsFor(datasetName, profileName);
                for (String facet : facets) {
                    candidates.add(datasetName + "." + profileName + "." + facet);
                }
            } catch (Exception e) {
                // Silently fail
            }

            return candidates.iterator();
        }
    }

    /// Completion candidates for facet names only.
    public static class FacetOnly implements Iterable<String> {

        /// Creates a new FacetOnly instance.
        public FacetOnly() {
        }

        @Override
        public Iterator<String> iterator() {
            return CANONICAL_FACET_NAMES.iterator();
        }
    }

    private static List<String> canonicalFacetNames() {
        List<String> names = new ArrayList<>();
        for (TestDataKind kind : TestDataKind.values()) {
            names.add(kind.name());
        }
        return names;
    }

    private static void addDatasetCandidates(List<String> candidates, List<String> datasets, String startsWith) {
        if (startsWith == null || startsWith.isEmpty()) {
            candidates.addAll(datasets);
            return;
        }
        for (String dataset : datasets) {
            if (dataset.startsWith(startsWith)) {
                candidates.add(dataset);
            }
        }
    }

    private static Optional<String> findUniqueDatasetMatch(String value, List<String> datasets) {
        String match = null;
        for (String dataset : datasets) {
            if (dataset.startsWith(value)) {
                if (match != null) {
                    return Optional.empty();
                }
                match = dataset;
            }
        }
        return Optional.ofNullable(match);
    }

    private record CompletionCatalog(
        Map<String, List<String>> profilesByDataset,
        Map<String, Map<String, List<String>>> facetsByDatasetProfile
    ) {
        static CompletionCatalog load(Path configDir) {
            Map<String, List<String>> profilesByDataset = new LinkedHashMap<>();
            Map<String, Map<String, List<String>>> facetsByDatasetProfile = new LinkedHashMap<>();
            try {
                TestDataSources config = new TestDataSources().configure(configDir);
                Catalog catalog = Catalog.of(config);
                for (DatasetEntry entry : catalog.datasets()) {
                    String datasetName = entry.name();
                    if (datasetName.contains(".")) {
                        continue;
                    }
                    List<String> profiles = new ArrayList<>();
                    Map<String, List<String>> facetsByProfile = new LinkedHashMap<>();
                    if (entry.profiles() != null) {
                        entry.profiles().forEach((profileName, profile) -> {
                            profiles.add(profileName);
                            facetsByProfile.put(profileName, canonicalFacetNames(profile));
                        });
                    }
                    profilesByDataset.put(datasetName, profiles);
                    facetsByDatasetProfile.put(datasetName, facetsByProfile);
                }
            } catch (Exception e) {
                // Silently fail - completion is best-effort
            }
            return new CompletionCatalog(profilesByDataset, facetsByDatasetProfile);
        }

        List<String> facetsFor(String datasetName, String profileName) {
            Map<String, List<String>> byProfile = facetsByDatasetProfile.get(datasetName);
            if (byProfile == null) {
                return List.of();
            }
            return byProfile.getOrDefault(profileName, List.of());
        }
    }

    private static List<String> canonicalFacetNames(DSProfile profile) {
        if (profile == null || profile.isEmpty()) {
            return List.of();
        }
        LinkedHashSet<String> names = new LinkedHashSet<>();
        for (String viewName : profile.keySet()) {
            Optional<TestDataKind> kind = TestDataKind.fromOptionalString(viewName);
            kind.ifPresent(value -> names.add(value.name()));
        }
        return new ArrayList<>(names);
    }
}
