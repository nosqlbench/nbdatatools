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

/// Provides dynamic completion candidates for VectorDataSpec parameters.
///
/// This class generates completion candidates that include:
/// - `file:` prefix suggestions
/// - `facet.` prefix suggestions with catalog datasets (default)
/// - `facet:` prefix suggestions for compatibility
/// - Common file extensions for vector data
///
/// For shell completion to work, users must first generate the completion script:
/// ```bash
/// source <(nbvectors generate-completion)
/// ```
public class VectorDataCompletionCandidates implements Iterable<String> {
    /// Creates a new VectorDataCompletionCandidates instance.
    public VectorDataCompletionCandidates() {}

    private static final Path DEFAULT_CONFIG_DIR = Path.of(
        System.getProperty("user.home"), ".config", "vectordata");

    private static final List<String> CANONICAL_FACET_NAMES = canonicalFacetNames();

    @Override
    public Iterator<String> iterator() {
        List<String> candidates = new ArrayList<>();

        // Prefix hints are always safe; prefix filtering trims as needed.
        candidates.add("file:");
        candidates.add("facet.");

        Optional<CompletionLineParser.ParsedLine> parsed = CompletionLineParser.parseFromEnv();
        String currentArg = parsed.map(CompletionLineParser.ParsedLine::currentArgPrefix).orElse("");

        if (currentArg.startsWith("http://") || currentArg.startsWith("https://")) {
            return candidates.iterator();
        }
        if (currentArg.startsWith("file:") || looksLikePath(currentArg)) {
            return candidates.iterator();
        }

        CompletionCatalog catalog = CompletionCatalog.load(DEFAULT_CONFIG_DIR);
        List<String> datasets = new ArrayList<>(catalog.profilesByDataset().keySet());
        if (datasets.isEmpty()) {
            return candidates.iterator();
        }

        String prefix = "";
        String rest = currentArg;
        if (rest.startsWith("facet.")) {
            prefix = "facet.";
            rest = rest.substring(6);
        } else if (rest.startsWith("facet:")) {
            prefix = "facet.";
            rest = rest.substring(6);
        }

        if (rest.isEmpty() || (!rest.contains(":") && !rest.contains("."))) {
            if (!rest.isEmpty()) {
                String separator = ".";
                Optional<String> uniqueMatch = findUniqueDatasetMatch(rest, datasets);
                if (uniqueMatch.isPresent()) {
                    String datasetName = uniqueMatch.get();
                    List<String> profiles = catalog.profilesByDataset().get(datasetName);
                    if (profiles != null && !profiles.isEmpty()) {
                        candidates.add(prefix + datasetName + separator);
                        return candidates.iterator();
                    }
                }
            }
            addDatasetCandidates(candidates, prefix, datasets);
            return candidates.iterator();
        }

        if (!rest.contains(":") && rest.contains(".") && hasDatasetPrefixMatch(rest, datasets)) {
            addDatasetCandidates(candidates, prefix, datasets, rest);
            return candidates.iterator();
        }

        String parseSeparator = rest.contains(".") ? "." : ":";
        String outputSeparator = ".";
        String[] parts = rest.split(Pattern.quote(parseSeparator), -1);
        if (parts.length < 2) {
            addDatasetCandidates(candidates, prefix, datasets);
            return candidates.iterator();
        }

        String datasetName = parts[0];
        List<String> profiles = catalog.profilesByDataset().get(datasetName);
        if (datasetName.isEmpty() || profiles == null || profiles.isEmpty()) {
            return candidates.iterator();
        }

        if (parts.length == 2) {
            String profilePrefix = parts[1];
            if (!profilePrefix.isEmpty()) {
                Optional<String> uniqueProfile = findUniqueProfileMatch(profilePrefix, profiles);
                if (uniqueProfile.isPresent()) {
                    String profileName = uniqueProfile.get();
                    candidates.add(prefix + datasetName + outputSeparator + profileName + outputSeparator);
                    return candidates.iterator();
                }
            }
            for (String profile : profiles) {
                candidates.add(prefix + datasetName + outputSeparator + profile + outputSeparator);
            }
            return candidates.iterator();
        }

        String profileName = parts[1];
        if (profileName.isEmpty() || !profiles.contains(profileName)) {
            return candidates.iterator();
        }

        List<String> facets = catalog.facetsFor(datasetName, profileName);
        for (String facet : facets) {
            candidates.add(prefix + datasetName + outputSeparator + profileName + outputSeparator + facet);
        }

        return candidates.iterator();
    }

    private static void addDatasetCandidates(List<String> candidates, String prefix, List<String> datasets) {
        for (String dataset : datasets) {
            candidates.add(prefix + dataset);
        }
    }

    private static void addDatasetCandidates(List<String> candidates, String prefix, List<String> datasets, String startsWith) {
        for (String dataset : datasets) {
            if (dataset.startsWith(startsWith)) {
                candidates.add(prefix + dataset);
            }
        }
    }

    private static boolean hasDatasetPrefixMatch(String prefix, List<String> datasets) {
        for (String dataset : datasets) {
            if (dataset.startsWith(prefix)) {
                return true;
            }
        }
        return false;
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

    private static Optional<String> findUniqueProfileMatch(String value, List<String> profiles) {
        String match = null;
        for (String profile : profiles) {
            if (profile.startsWith(value)) {
                if (match != null) {
                    return Optional.empty();
                }
                match = profile;
            }
        }
        return Optional.ofNullable(match);
    }

    private static boolean looksLikePath(String value) {
        return value.startsWith("/")
            || value.startsWith("./")
            || value.startsWith("../")
            || value.startsWith("~");
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

    /// Completion candidates that only include facet specifications.
    /// Use this when only facets are valid (not files or URLs).
    public static class FacetsOnly implements Iterable<String> {
        /// Creates a new FacetsOnly instance.
        public FacetsOnly() {}

        @Override
        public Iterator<String> iterator() {
            List<String> candidates = new ArrayList<>();
            for (String facet : CANONICAL_FACET_NAMES) {
                candidates.add(facet);
            }
            return candidates.iterator();
        }
    }

    /// Completion candidates that include common vector file extensions.
    /// Useful for commands that primarily work with local files.
    public static class WithFileExtensions implements Iterable<String> {
        /// Creates a new WithFileExtensions instance.
        public WithFileExtensions() {}

        @Override
        public Iterator<String> iterator() {
            List<String> candidates = new ArrayList<>();

            // Common vector file patterns
            candidates.add("*.fvec");
            candidates.add("*.ivec");
            candidates.add("*.bvec");
            candidates.add("*.hdf5");
            candidates.add("*.parquet");

            // Add prefix hints
            candidates.add("file:");
            candidates.add("facet.");
            candidates.add("facet:");

            CompletionCatalog catalog = CompletionCatalog.load(DEFAULT_CONFIG_DIR);
            for (Map.Entry<String, List<String>> datasetEntry : catalog.profilesByDataset().entrySet()) {
                String datasetName = datasetEntry.getKey();
                for (String profileName : datasetEntry.getValue()) {
                    List<String> facets = catalog.facetsFor(datasetName, profileName);
                    for (String facet : facets) {
                        candidates.add("facet." + datasetName + "." + profileName + "." + facet);
                    }
                }
            }

            return candidates.iterator();
        }
    }

    private static List<String> canonicalFacetNames() {
        List<String> names = new ArrayList<>();
        for (TestDataKind kind : TestDataKind.values()) {
            names.add(kind.name());
        }
        return names;
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
