package io.nosqlbench.command.datasets.subcommands;

/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
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

import io.nosqlbench.command.common.CommandLineFormatter;
import io.nosqlbench.command.common.DistancesFileOption;
import io.nosqlbench.common.types.VectorFileExtension;
import io.nosqlbench.vectordata.layout.FInterval;
import io.nosqlbench.vectordata.layout.FProfiles;
import io.nosqlbench.vectordata.layout.FView;
import io.nosqlbench.vectordata.layout.FWindow;
import io.nosqlbench.vectordata.layout.TestGroupLayout;
import io.nosqlbench.vectordata.spec.attributes.RootGroupAttributes;
import io.nosqlbench.vectordata.spec.datasets.types.ViewKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/// Analyze a dataset.yaml and suggest commands to fill in missing dataset facets.
@CommandLine.Command(name = "plan",
    header = "Inspect dataset.yaml and suggest commands to generate missing facets",
    description = "Reads a dataset.yaml file, checks which referenced vector assets exist, and emits\n" +
        "the nbdatatools command invocations needed to create any missing vectors, indices,\n" +
        "or distance files.")
public class CMD_datasets_plan implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_datasets_plan.class);
    private static final Pattern REMOTE_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]*://.+");

    @CommandLine.Parameters(paramLabel = "PATH",
        description = "Dataset directory or explicit dataset.yaml",
        arity = "0..1",
        defaultValue = ".")
    private Path target;

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    private record FacetRecord(
        String profile,
        String viewName,
        ViewKind kind,
        String sourceSpec,
        Path absolutePath,
        boolean remote,
        boolean exists,
        FView view,
        FProfiles profileConfig
    ) {
        String key() {
            if (absolutePath != null) {
                return absolutePath.normalize().toString();
            }
            return sourceSpec;
        }
    }

    private static final class Suggestion {
        private final String command;
        private final LinkedHashSet<String> rationales = new LinkedHashSet<>();

        private Suggestion(String command) {
            this.command = command;
        }

        void addRationale(String rationale) {
            rationales.add(rationale);
        }

        String command() {
            return command;
        }

        String rationaleSummary() {
            return String.join("; ", rationales);
        }
    }

    private static final class ProfileSummary {
        private final String name;
        private final Map<ViewKind, List<FacetRecord>> byKind = new LinkedHashMap<>();
        private final Map<String, FacetRecord> byViewName = new LinkedHashMap<>();

        ProfileSummary(String name) {
            this.name = name;
        }

        void addFacet(FacetRecord record) {
            if (record.kind != null) {
                byKind.computeIfAbsent(record.kind, k -> new ArrayList<>()).add(record);
            }
            byViewName.put(record.viewName, record);
        }

        Optional<FacetRecord> first(ViewKind kind) {
            List<FacetRecord> items = byKind.get(kind);
            if (items == null || items.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(items.get(0));
        }

        Collection<FacetRecord> allFacets() {
            return byViewName.values();
        }
    }

    @Override
    public Integer call() {
        // Print the command line being executed
        CommandLineFormatter.printCommandLine(spec);
        spec.commandLine().getOut().println();

        Path resolved = target.toAbsolutePath().normalize();
        Path datasetYaml = Files.isDirectory(resolved) ? resolved.resolve("dataset.yaml") : resolved;
        if (!Files.exists(datasetYaml)) {
            spec.commandLine().getErr().printf(
                "Dataset descriptor not found at %s%n",
                datasetYaml.toAbsolutePath()
            );
            return 1;
        }

        final Path datasetRoot = (datasetYaml.getParent() != null)
            ? datasetYaml.getParent()
            : Paths.get(".");

        TestGroupLayout layout;
        try {
            layout = TestGroupLayout.load(datasetYaml);
        } catch (RuntimeException rte) {
            spec.commandLine().getErr().printf("Unable to parse %s: %s%n",
                datasetYaml.toAbsolutePath(), rte.getMessage());
            logger.debug("dataset.yaml parse failure", rte);
            return 1;
        }

        Map<String, ProfileSummary> summaries = new LinkedHashMap<>();
        List<FacetRecord> allFacets = new ArrayList<>();

        layout.profiles().profiles().forEach((profileName, fProfiles) -> {
            ProfileSummary summary = summaries.computeIfAbsent(profileName, ProfileSummary::new);
            Map<String, FView> views = fProfiles.views();
            views.forEach((viewName, fView) -> {
                ViewKind viewKind = ViewKind.fromName(viewName).orElse(null);
                String sourceSpec = fView.source().inpath();
                boolean remote = REMOTE_PATTERN.matcher(sourceSpec).matches();

                final Path absolutePath;
                if (!remote) {
                    Path srcPath = Paths.get(sourceSpec);
                    absolutePath = srcPath.isAbsolute()
                        ? srcPath.normalize()
                        : datasetRoot.resolve(srcPath).normalize();
                } else {
                    absolutePath = null;
                }

                boolean exists = remote;
                if (!remote && absolutePath != null) {
                    exists = Files.exists(absolutePath);
                    logger.debug("Checking existence of {} for profile '{}' view '{}': {}",
                        absolutePath, profileName, viewName, exists ? "EXISTS" : "MISSING");
                }

                FacetRecord record = new FacetRecord(
                    profileName,
                    viewName,
                    viewKind,
                    sourceSpec,
                    absolutePath,
                    remote,
                    exists,
                    fView,
                    fProfiles
                );
                summary.addFacet(record);
                allFacets.add(record);
            });
        });

        List<FacetRecord> missing = allFacets.stream()
            .filter(f -> !f.exists)
            .sorted((a, b) -> {
                int cmp = a.profile.compareToIgnoreCase(b.profile);
                if (cmp != 0) return cmp;
                String ak = facetLabel(a);
                String bk = facetLabel(b);
                cmp = ak.compareToIgnoreCase(bk);
                if (cmp != 0) return cmp;
                return displayPath(datasetRoot, a).compareToIgnoreCase(displayPath(datasetRoot, b));
            })
            .collect(Collectors.toList());

        if (missing.isEmpty()) {
            spec.commandLine().getOut().printf(
                "All declared facets in %s are present.%n",
                datasetYaml.toAbsolutePath()
            );
            return 0;
        }

        spec.commandLine().getOut().printf("Dataset: %s%n", datasetYaml.toAbsolutePath());
        spec.commandLine().getOut().println("Missing facets:");
        missing.forEach(facet -> spec.commandLine().getOut().printf(
            " - profile '%s' facet '%s' -> %s%n",
            facet.profile,
            facetLabel(facet),
            displayPath(datasetRoot, facet)
        ));
        spec.commandLine().getOut().println();

        LinkedHashMap<String, Suggestion> suggestionByCommand = new LinkedHashMap<>();

        addVectorSuggestions(
            summaries,
            datasetRoot,
            ViewKind.base,
            "Generate base vectors for profile(s) %s",
            suggestionByCommand
        );

        addVectorSuggestions(
            summaries,
            datasetRoot,
            ViewKind.query,
            "Generate query vectors for profile(s) %s",
            suggestionByCommand
        );

        addKnnSuggestions(
            summaries,
            datasetRoot,
            layout.attributes(),
            suggestionByCommand
        );

        if (suggestionByCommand.isEmpty()) {
            spec.commandLine().getOut().println("No automated command suggestions available for the missing facets.");
            return 0;
        }

        List<Suggestion> suggestions = new ArrayList<>(suggestionByCommand.values());
        spec.commandLine().getOut().println("Suggested commands:");
        spec.commandLine().getOut().println();

        for (int i = 0; i < suggestions.size(); i++) {
            Suggestion suggestion = suggestions.get(i);
            spec.commandLine().getOut().printf("# %d. %s%n", i + 1, suggestion.rationaleSummary());

            // Format command for easy terminal copy-paste with line wrapping at 100 chars
            String formattedCommand = formatCommandForTerminal(suggestion.command());
            spec.commandLine().getOut().println(formattedCommand);
            spec.commandLine().getOut().println();
        }

        return 0;
    }

    /// Format a command for terminal copy-paste with proper line wrapping
    /// @param command the command to format
    /// @return formatted command with backslash line continuations
    private static String formatCommandForTerminal(String command) {
        final int maxLineLength = 100;

        // If command is short enough, return as-is
        if (command.length() <= maxLineLength) {
            return command;
        }

        // Split command into parts at option boundaries (--option)
        String[] parts = command.split("(?= --)");
        StringBuilder formatted = new StringBuilder();
        StringBuilder currentLine = new StringBuilder();

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];

            // First part is the command itself (e.g., "compute knn")
            if (i == 0) {
                currentLine.append(part);
                continue;
            }

            // Check if adding this part would exceed line length
            if (currentLine.length() + part.length() + 2 > maxLineLength) {
                // Finish current line with backslash
                formatted.append(currentLine).append(" \\").append(System.lineSeparator());
                currentLine = new StringBuilder("  ").append(part.trim());
            } else {
                currentLine.append(" ").append(part.trim());
            }
        }

        // Append final line
        if (currentLine.length() > 0) {
            formatted.append(currentLine);
        }

        return formatted.toString();
    }

    private static String facetLabel(FacetRecord record) {
        return record.kind != null ? record.kind.name() : record.viewName;
    }

    private static void addVectorSuggestions(
        Map<String, ProfileSummary> summaries,
        Path datasetRoot,
        ViewKind viewKind,
        String rationaleTemplate,
        Map<String, Suggestion> suggestionByCommand
    ) {
        Map<String, List<FacetRecord>> missingByPath = new LinkedHashMap<>();
        summaries.values().forEach(summary -> {
            List<FacetRecord> facets = summary.byKind.get(viewKind);
            if (facets == null) {
                return;
            }
            for (FacetRecord record : facets) {
                if (!record.exists) {
                    missingByPath.computeIfAbsent(record.key(), k -> new ArrayList<>()).add(record);
                }
            }
        });

        // Filter out groups where ANY record shows the file exists
        // This handles cases where multiple profiles reference the same file with different windows
        missingByPath.entrySet().removeIf(entry -> {
            boolean anyExists = entry.getValue().stream().anyMatch(rec -> rec.exists);
            if (anyExists) {
                logger.debug("Skipping suggestion for {} because file exists", entry.getKey());
            }
            return anyExists;
        });

        missingByPath.values().forEach(group -> {
            FacetRecord reference = group.get(0);
            String displayPath = displayPath(datasetRoot, reference);
            VectorFileExtension extension = detectExtension(reference);

            String vectorType = extension != null
                ? extension.getDataType().getSimpleName()
                : "<vector-type>";

            String format = extension != null
                ? extension.getFileType().name().toLowerCase(Locale.ROOT)
                : "<format>";

            // Find the MAXIMUM count needed across all profiles that reference this file
            // This ensures we generate enough vectors to satisfy all profiles
            OptionalLong maxCount = group.stream()
                .map(rec -> estimateCount(rec.view().window()))
                .filter(OptionalLong::isPresent)
                .mapToLong(OptionalLong::getAsLong)
                .max();
            String count = maxCount.isPresent() ? Long.toString(maxCount.getAsLong()) : "<count>";

            // Try to extract dimension from existing files in the profile
            ProfileSummary summary = summaries.get(reference.profile);
            OptionalLong dimension = summary != null ? findDimensionInProfile(summary) : OptionalLong.empty();

            // Use 128 as default dimension (common for embeddings) with a note
            String dimensionStr;
            boolean usedDefaultDimension = false;
            if (dimension.isPresent()) {
                dimensionStr = Long.toString(dimension.getAsLong());
            } else {
                dimensionStr = "128";
                usedDefaultDimension = true;
            }

            String command = String.format(
                "nbvectors generate vectors --output %s --type %s --format %s --dimension %s --count %s",
                shellPath(displayPath),
                vectorType,
                format,
                dimensionStr,
                count
            );

            // Build rationale explaining which profiles need this file
            String profiles = group.stream()
                .map(f -> "'" + f.profile + "'")
                .distinct()
                .sorted(String::compareToIgnoreCase)
                .collect(Collectors.joining(", "));

            String rationale = String.format(rationaleTemplate, profiles);

            suggestionByCommand
                .computeIfAbsent(command, Suggestion::new)
                .addRationale(rationale);
        });
    }

    private static void addKnnSuggestions(
        Map<String, ProfileSummary> summaries,
        Path datasetRoot,
        RootGroupAttributes attributes,
        Map<String, Suggestion> suggestionByCommand
    ) {
        summaries.values().forEach(summary -> {
            Optional<FacetRecord> indices = summary.first(ViewKind.indices);
            Optional<FacetRecord> distances = summary.first(ViewKind.neighbors);

            boolean indicesMissing = indices.isPresent() && !indices.get().exists;
            boolean distancesMissing = distances.isPresent() && !distances.get().exists;

            // Only suggest KNN computation if BOTH indices and distances are missing
            // If only one is missing, that's a broken/inconsistent state - don't suggest automatic repair
            if (!indicesMissing || !distancesMissing) {
                if (indicesMissing || distancesMissing) {
                    logger.debug("Skipping KNN suggestion for profile '{}' - only one output is missing (indices missing={}, distances missing={})",
                        summary.name, indicesMissing, distancesMissing);
                }
                return;
            }

            Optional<FacetRecord> base = summary.first(ViewKind.base);
            Optional<FacetRecord> query = summary.first(ViewKind.query);

            String basePath = base.map(f -> displayPath(datasetRoot, f)).orElse("<base-file>");
            String queryPath = query.map(f -> displayPath(datasetRoot, f)).orElse("<query-file>");
            String indicesPath = indices.map(f -> displayPath(datasetRoot, f)).orElse("<indices-file>");

            String distancesPath = distances
                .map(f -> displayPath(datasetRoot, f))
                .orElseGet(() -> deriveDistancesPath(indicesPath));

            // Extract range/window from base vectors for --range option
            String rangeOption = "";
            if (base.isPresent() && base.get().view != null && base.get().view.window() != null) {
                FWindow window = base.get().view.window();
                if (window != FWindow.ALL && !window.intervals().isEmpty()) {
                    OptionalLong count = estimateCount(window);
                    if (count.isPresent()) {
                        rangeOption = " --range " + count.getAsLong();
                    }
                }
            }

            // Check if base vectors are normalized (for optimal metric selection)
            boolean baseIsNormalized = false;
            if (base.isPresent() && base.get().exists && base.get().absolutePath != null) {
                try {
                    baseIsNormalized = io.nosqlbench.command.compute.VectorNormalizationDetector.areVectorsNormalized(base.get().absolutePath);
                } catch (Exception e) {
                    logger.debug("Could not detect normalization for {}: {}", base.get().absolutePath, e.getMessage());
                }
            }

            // Try to extract k value from multiple sources
            OptionalLong kValue = OptionalLong.empty();
            String kSource = null;

            // First, try to get maxk from profile configuration
            if (indices.isPresent() && indices.get().profileConfig != null && indices.get().profileConfig.maxk() != null) {
                kValue = OptionalLong.of(indices.get().profileConfig.maxk());
                kSource = "profile maxk";
            }

            // Second, try to extract k from existing indices file
            if (!kValue.isPresent()) {
                kValue = findKInProfile(summary);
                if (kValue.isPresent()) {
                    kSource = "existing indices file";
                }
            }

            // Use 100 as default k value (common for ANN benchmarks)
            boolean usedDefaultK = false;
            String kStr;
            if (kValue.isPresent()) {
                kStr = Long.toString(kValue.getAsLong());
            } else {
                kStr = "100";
                usedDefaultK = true;
                kSource = "default";
            }

            // Extract metric from attributes or use optimal default based on normalization
            String metric;
            boolean usedDefaultMetric = false;
            boolean usedOptimizedMetric = false;
            if (attributes != null && attributes.distance_function() != null) {
                String specifiedMetric = attributes.distance_function().name();
                // If COSINE specified but vectors are normalized, recommend DOT_PRODUCT instead
                if ("COSINE".equals(specifiedMetric) && baseIsNormalized) {
                    metric = "DOT_PRODUCT";
                    usedOptimizedMetric = true;
                } else {
                    metric = specifiedMetric;
                }
            } else {
                // No metric specified - choose optimal default
                metric = baseIsNormalized ? "DOT_PRODUCT" : "EUCLIDEAN";
                usedDefaultMetric = true;
            }

            String command = String.format(
                "nbvectors compute knn --base %s --query %s%s --indices %s --distances %s --neighbors %s --metric %s",
                shellPath(basePath),
                shellPath(queryPath),
                rangeOption,
                shellPath(indicesPath),
                shellPath(distancesPath),
                kStr,
                metric
            );

            String rationale = String.format(
                "Compute ground-truth neighbors for profile '%s'",
                summary.name
            );

            suggestionByCommand
                .computeIfAbsent(command, Suggestion::new)
                .addRationale(rationale);
        });
    }

    private static String deriveDistancesPath(String indicesPath) {
        if ("<indices-file>".equals(indicesPath)) {
            return "<distances-file>";
        }
        Path indices = Paths.get(indicesPath);
        Path parent = indices.getParent();
        String fileName = indices.getFileName().toString();

        String replacement;
        if (fileName.contains("indices")) {
            replacement = fileName.replace("indices", "distances");
        } else if (fileName.contains("neighbors")) {
            replacement = fileName.replace("neighbors", "distances");
        } else if (fileName.contains("idx")) {
            replacement = fileName.replace("idx", "dist");
        } else {
            DistancesFileOption helper = new DistancesFileOption();
            helper.setIndicesPath(indices);
            return helper.getNormalizedDistancesPath().toString();
        }

        Path derived = parent != null ? parent.resolve(replacement) : Paths.get(replacement);
        return derived.normalize().toString();
    }

    private static VectorFileExtension detectExtension(FacetRecord record) {
        Path path = record.absolutePath;
        String source = record.sourceSpec;
        String candidateName = path != null ? path.getFileName().toString() : source;
        int dot = candidateName.lastIndexOf('.');
        if (dot < 0) {
            return null;
        }
        String ext = candidateName.substring(dot);
        return VectorFileExtension.fromExtension(ext);
    }

    private static OptionalLong estimateCount(FWindow window) {
        if (window == null) {
            return OptionalLong.empty();
        }
        List<FInterval> intervals = window.intervals();
        if (intervals == null || intervals.isEmpty()) {
            return OptionalLong.empty();
        }
        long total = 0L;
        for (FInterval interval : intervals) {
            long min = interval.minIncl();
            long max = interval.maxExcl();
            if (min < 0 || max < 0) {
                return OptionalLong.empty();
            }
            total += (max - min);
        }
        if (total <= 0) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(total);
    }

    private static String displayPath(Path datasetRoot, FacetRecord record) {
        if (record.absolutePath == null) {
            return record.sourceSpec;
        }
        Path normalizedRoot = datasetRoot != null ? datasetRoot.toAbsolutePath().normalize() : Paths.get(".");
        Path absolute = record.absolutePath.toAbsolutePath().normalize();
        if (absolute.startsWith(normalizedRoot)) {
            Path relative = normalizedRoot.relativize(absolute);
            if (relative.toString().isEmpty()) {
                return absolute.getFileName().toString();
            }
            return relative.toString();
        }
        return absolute.toString();
    }

    private static String shellPath(String path) {
        if (path.contains(" ") || path.contains("\"")) {
            return "\"" + path.replace("\"", "\\\"") + "\"";
        }
        return path;
    }

    /// Extract the dimension from an existing vector file if it exists
    /// @param record the facet record that might have an existing file
    /// @return the dimension if available, empty otherwise
    private static OptionalLong extractDimension(FacetRecord record) {
        if (!record.exists || record.absolutePath == null) {
            return OptionalLong.empty();
        }
        try {
            io.nosqlbench.vectordata.discovery.TestDataGroup group =
                new io.nosqlbench.vectordata.discovery.TestDataGroup(record.absolutePath);
            try {
                io.nosqlbench.vectordata.discovery.TestDataView view =
                    group.profile("default");

                // Try to get dimension from base or query vectors
                if (record.kind == ViewKind.base || record.kind == ViewKind.query) {
                    io.nosqlbench.vectordata.spec.datasets.types.DatasetView<?> dataView =
                        record.kind == ViewKind.base ? view.getBaseVectors().orElse(null) : view.getQueryVectors().orElse(null);
                    if (dataView != null) {
                        return OptionalLong.of(dataView.getVectorDimensions());
                    }
                }
            } finally {
                group.close();
            }
        } catch (Exception e) {
            logger.debug("Could not extract dimension from {}: {}", record.absolutePath, e.getMessage());
        }
        return OptionalLong.empty();
    }

    /// Extract the k (neighbors) value from an existing indices file if it exists
    /// @param record the facet record that might have an existing indices file
    /// @return the k value if available, empty otherwise
    private static OptionalLong extractNeighborsK(FacetRecord record) {
        if (!record.exists || record.absolutePath == null) {
            return OptionalLong.empty();
        }
        try {
            io.nosqlbench.vectordata.discovery.TestDataGroup group =
                new io.nosqlbench.vectordata.discovery.TestDataGroup(record.absolutePath);
            try {
                io.nosqlbench.vectordata.discovery.TestDataView view =
                    group.profile("default");

                // Get k from indices
                if (record.kind == ViewKind.indices) {
                    io.nosqlbench.vectordata.spec.datasets.types.NeighborIndices indices =
                        view.getNeighborIndices().orElse(null);
                    if (indices != null) {
                        return OptionalLong.of(indices.getVectorDimensions());
                    }
                }
            } finally {
                group.close();
            }
        } catch (Exception e) {
            logger.debug("Could not extract k from {}: {}", record.absolutePath, e.getMessage());
        }
        return OptionalLong.empty();
    }

    /// Try to find dimension from any existing vector file in the profile
    /// @param summary the profile summary
    /// @return the dimension if found in any existing file
    private static OptionalLong findDimensionInProfile(ProfileSummary summary) {
        // Try base vectors first
        Optional<FacetRecord> base = summary.first(ViewKind.base);
        if (base.isPresent() && base.get().exists) {
            OptionalLong dim = extractDimension(base.get());
            if (dim.isPresent()) {
                return dim;
            }
        }

        // Try query vectors
        Optional<FacetRecord> query = summary.first(ViewKind.query);
        if (query.isPresent() && query.get().exists) {
            OptionalLong dim = extractDimension(query.get());
            if (dim.isPresent()) {
                return dim;
            }
        }

        return OptionalLong.empty();
    }

    /// Try to find k value from any existing indices file in the profile
    /// @param summary the profile summary
    /// @return the k value if found in any existing file
    private static OptionalLong findKInProfile(ProfileSummary summary) {
        Optional<FacetRecord> indices = summary.first(ViewKind.indices);
        if (indices.isPresent() && indices.get().exists) {
            return extractNeighborsK(indices.get());
        }
        return OptionalLong.empty();
    }
}
