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
        FView view
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
                }

                FacetRecord record = new FacetRecord(
                    profileName,
                    viewName,
                    viewKind,
                    sourceSpec,
                    absolutePath,
                    remote,
                    exists,
                    fView
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
        for (int i = 0; i < suggestions.size(); i++) {
            Suggestion suggestion = suggestions.get(i);
            spec.commandLine().getOut().printf(
                " %d. %s%n    %s%n",
                i + 1,
                suggestion.rationaleSummary(),
                suggestion.command()
            );
        }

        return 0;
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

            OptionalLong countGuess = estimateCount(reference.view().window());
            String count = countGuess.isPresent() ? Long.toString(countGuess.getAsLong()) : "<count>";

            String command = String.format(
                "generate vectors --output %s --type %s --format %s --dimension <dimension> --count %s",
                shellPath(displayPath),
                vectorType,
                format,
                count
            );

            String rationale = String.format(
                rationaleTemplate,
                group.stream()
                    .map(f -> "'" + f.profile + "'")
                    .distinct()
                    .sorted(String::compareToIgnoreCase)
                    .collect(Collectors.joining(", "))
            );

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

            if (!indicesMissing && !distancesMissing) {
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

            boolean needForce = indices.isPresent() && indices.get().exists
                || distances.isPresent() && distances.get().exists;

            String metric = attributes != null && attributes.distance_function() != null
                ? attributes.distance_function().name()
                : "<metric>";

            String command = String.format(
                "compute knn --base %s --query %s --indices %s --distances %s --neighbors <k> --metric %s%s",
                shellPath(basePath),
                shellPath(queryPath),
                shellPath(indicesPath),
                shellPath(distancesPath),
                metric,
                needForce ? " --force" : ""
            );

            String rationale = String.format(
                "Recompute ground-truth neighbors for profile '%s'%s",
                summary.name,
                needForce ? " (overwrites existing outputs)" : ""
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
}
