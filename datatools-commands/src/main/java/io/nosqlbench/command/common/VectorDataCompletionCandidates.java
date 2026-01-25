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
import java.util.LinkedHashMap;
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

    private static final Path DEFAULT_CONFIG_DIR = Path.of(
        System.getProperty("user.home"), ".config", "vectordata");

    /// Standard facet names for completion
    private static final List<String> FACET_NAMES = List.of(
        "base", "query", "indices", "distances"
    );

    @Override
    public Iterator<String> iterator() {
        List<String> candidates = new ArrayList<>();

        // Prefix hints are always safe; prefix filtering trims as needed.
        candidates.add("file:");
        candidates.add("facet.");
        candidates.add("facet:");

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
            prefix = "facet:";
            rest = rest.substring(6);
        }

        if (rest.isEmpty() || (!rest.contains(":") && !rest.contains("."))) {
            if (!rest.isEmpty()) {
                String separator = prefix.endsWith(":") ? ":" : ".";
                Optional<String> uniqueMatch = findUniqueDatasetMatch(rest, datasets);
                if (uniqueMatch.isPresent()) {
                    String datasetName = uniqueMatch.get();
                    List<String> profiles = catalog.profilesByDataset().get(datasetName);
                    if (profiles != null && !profiles.isEmpty()) {
                        candidates.add(prefix + datasetName + separator);
                        for (String profile : profiles) {
                            candidates.add(prefix + datasetName + separator + profile + separator);
                        }
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

        String separator = rest.contains(":") ? ":" : ".";
        String[] parts = rest.split(Pattern.quote(separator), -1);
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
                    candidates.add(prefix + datasetName + separator + profileName + separator);
                    for (String facet : FACET_NAMES) {
                        candidates.add(prefix + datasetName + separator + profileName + separator + facet);
                    }
                    return candidates.iterator();
                }
            }
            for (String profile : profiles) {
                candidates.add(prefix + datasetName + separator + profile + separator);
            }
            return candidates.iterator();
        }

        String profileName = parts[1];
        if (profileName.isEmpty() || !profiles.contains(profileName)) {
            return candidates.iterator();
        }

        for (String facet : FACET_NAMES) {
            candidates.add(prefix + datasetName + separator + profileName + separator + facet);
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

    private record CompletionCatalog(Map<String, List<String>> profilesByDataset) {
        static CompletionCatalog load(Path configDir) {
            Map<String, List<String>> profilesByDataset = new LinkedHashMap<>();
            try {
                TestDataSources config = new TestDataSources().configure(configDir);
                Catalog catalog = Catalog.of(config);
                for (DatasetEntry entry : catalog.datasets()) {
                    String datasetName = entry.name();
                    List<String> profiles = new ArrayList<>();
                    if (entry.profiles() != null) {
                        profiles.addAll(entry.profiles().keySet());
                    }
                    profilesByDataset.put(datasetName, profiles);
                }
            } catch (Exception e) {
                // Silently fail - completion is best-effort
            }
            return new CompletionCatalog(profilesByDataset);
        }
    }

    /// Completion candidates that only include facet specifications.
    /// Use this when only facets are valid (not files or URLs).
    public static class FacetsOnly implements Iterable<String> {
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
                            for (String facet : FACET_NAMES) {
                            candidates.add("facet." + datasetName + "." + profileName + "." + facet);
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

    /// Completion candidates that include common vector file extensions.
    /// Useful for commands that primarily work with local files.
    public static class WithFileExtensions implements Iterable<String> {
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

            try {
                TestDataSources config = new TestDataSources().configure(DEFAULT_CONFIG_DIR);
                Catalog catalog = Catalog.of(config);

                for (DatasetEntry entry : catalog.datasets()) {
                    String datasetName = entry.name();

                    if (entry.profiles() != null) {
                        for (String profileName : entry.profiles().keySet()) {
                            for (String facet : FACET_NAMES) {
                                candidates.add("facet." + datasetName + "." + profileName + "." + facet);
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
}
