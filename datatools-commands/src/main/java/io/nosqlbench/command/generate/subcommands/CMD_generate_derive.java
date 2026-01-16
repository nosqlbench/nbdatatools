package io.nosqlbench.command.generate.subcommands;

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

import io.nosqlbench.vectordata.discovery.TestDataGroup;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import io.nosqlbench.vshapes.extract.BestFitSelector;
import io.nosqlbench.vshapes.extract.DatasetModelExtractor;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.vshapes.model.VectorSpaceModelConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/// Derive a model-based dataset from an existing dataset.
///
/// This command takes an existing dataset directory (with base vectors), extracts a
/// statistical model from the base vectors, and creates a new dataset directory that
/// uses virtdata (model-based generation) for its vectors.
///
/// The derived dataset can generate unlimited vectors on-the-fly from the statistical
/// model, making it useful for:
/// - Creating larger test datasets without storing all vectors
/// - Generating synthetic data with the same statistical properties as real data
/// - Reproducible benchmark datasets
///
/// ## Usage
///
/// ```bash
/// # Derive from local dataset
/// nbvectors generate derive \
///     --source /path/to/source-dataset \
///     --target /path/to/derived-dataset \
///     --count 1000000
///
/// # Derive with specific profile and custom name
/// nbvectors generate derive \
///     --source /path/to/source-dataset \
///     --profile train_large \
///     --target /path/to/derived-dataset \
///     --name my-synthetic-dataset \
///     --count 500000
/// ```
///
/// ## Output
///
/// Creates a new dataset directory with:
/// - `model.json` - The extracted VectorSpaceModel
/// - `dataset.yaml` - Configuration using virtdata source
///
/// The dataset.yaml will look like:
/// ```yaml
/// attributes:
///   derived_from: source-dataset
///   distance_function: COSINE  # (copied from source)
///   generation_mode: virtdata
///
/// profiles:
///   default:
///     base_vectors:
///       source: model.json
///       window: 0..1000000
/// ```
@CommandLine.Command(
    name = "derive",
    header = "Derive a model-based dataset from an existing dataset",
    description = "Extracts a statistical model from source dataset and creates a new virtdata-based dataset.",
    exitCodeList = {
        "0: Success",
        "1: Source not found or invalid",
        "2: Error during extraction or writing"
    }
)
public class CMD_generate_derive implements Callable<Integer> {

    private static final Logger logger = LogManager.getLogger(CMD_generate_derive.class);

    // ANSI color codes
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_BOLD = "\u001B[1m";
    private static final String ANSI_DIM = "\u001B[2m";
    private static final String ANSI_CYAN = "\u001B[96m";
    private static final String ANSI_GREEN = "\u001B[92m";
    private static final String ANSI_YELLOW = "\u001B[93m";

    @CommandLine.Option(
        names = {"--source", "-s"},
        description = "Source dataset directory (must contain dataset.yaml)",
        required = true
    )
    private Path sourceDir;

    @CommandLine.Option(
        names = {"--target", "-t"},
        description = "Target directory for derived dataset",
        required = true
    )
    private Path targetDir;

    @CommandLine.Option(
        names = {"--profile", "-p"},
        description = "Profile name to use from source dataset (default: default)"
    )
    private String profileName = "default";

    @CommandLine.Option(
        names = {"--count", "-n"},
        description = "Number of vectors for derived dataset (default: same as source)"
    )
    private Long vectorCount;

    @CommandLine.Option(
        names = {"--name"},
        description = "Name for the derived dataset (default: derived from source name)"
    )
    private String datasetName;

    @CommandLine.Option(
        names = {"--sample"},
        description = "Maximum vectors to sample from source for model extraction (default: all)"
    )
    private Integer sampleSize;

    @CommandLine.Option(
        names = {"--force", "-f"},
        description = "Overwrite target directory if it exists"
    )
    private boolean force = false;

    @CommandLine.Option(
        names = {"--verbose", "-v"},
        description = "Show detailed progress"
    )
    private boolean verbose = false;

    @CommandLine.Option(
        names = {"--model-type"},
        description = "Model fitting strategy: auto, bounded, pearson, empirical (default: auto)"
    )
    private String modelType = "auto";

    @Override
    public Integer call() {
        try {
            // Validate source
            if (!Files.exists(sourceDir)) {
                System.err.println("Error: Source directory not found: " + sourceDir);
                return 1;
            }

            Path sourceYaml = sourceDir.resolve("dataset.yaml");
            if (!Files.exists(sourceYaml)) {
                System.err.println("Error: No dataset.yaml found in source directory: " + sourceDir);
                return 1;
            }

            // Validate target
            if (Files.exists(targetDir)) {
                if (!force) {
                    System.err.println("Error: Target directory already exists: " + targetDir);
                    System.err.println("Use --force to overwrite.");
                    return 1;
                }
                // Clean existing target
                deleteRecursively(targetDir);
            }

            // Load source dataset
            printHeader();
            System.out.println(ANSI_CYAN + "Loading source dataset..." + ANSI_RESET);

            TestDataGroup sourceDataset = new TestDataGroup(sourceDir);
            TestDataView profile = sourceDataset.profile(profileName);

            if (profile == null) {
                System.err.println("Error: Profile '" + profileName + "' not found in source dataset");
                System.err.println("Available profiles: " + sourceDataset.getProfileNames());
                return 1;
            }

            // Get base vectors
            Optional<BaseVectors> baseVectorsOpt = profile.getBaseVectors();
            if (baseVectorsOpt.isEmpty()) {
                System.err.println("Error: No base vectors found in profile '" + profileName + "'");
                return 1;
            }

            BaseVectors baseVectors = baseVectorsOpt.get();
            int dimensions = baseVectors.getVectorDimensions();
            int sourceVectorCount = baseVectors.getCount();

            System.out.printf("  Source: %s%n", sourceDir.getFileName());
            System.out.printf("  Profile: %s%n", profileName);
            System.out.printf("  Dimensions: %d%n", dimensions);
            System.out.printf("  Source vectors: %,d%n", sourceVectorCount);

            // Determine vector count for derived dataset
            long derivedCount = vectorCount != null ? vectorCount : sourceVectorCount;

            // Extract model
            System.out.println();
            System.out.println(ANSI_CYAN + "Extracting statistical model..." + ANSI_RESET);

            int samplesToUse = sampleSize != null ? Math.min(sampleSize, sourceVectorCount) : sourceVectorCount;
            if (verbose) {
                System.out.printf("  Sampling %,d vectors for model extraction%n", samplesToUse);
            }

            // Load vectors for extraction
            float[][] vectors = loadVectors(baseVectors, samplesToUse);

            // Select model fitter
            BestFitSelector selector = selectFitter(modelType);

            // Extract model
            DatasetModelExtractor extractor = new DatasetModelExtractor(selector, derivedCount);

            if (verbose) {
                System.out.printf("  Extracting model from %d dimensions...%n", dimensions);
            }

            VectorSpaceModel model = extractor.extractVectorModel(vectors);

            if (verbose) {
                printModelSummary(model);
            }

            // Create target directory
            System.out.println();
            System.out.println(ANSI_CYAN + "Creating derived dataset..." + ANSI_RESET);

            Files.createDirectories(targetDir);

            // Save model
            Path modelPath = targetDir.resolve("model.json");
            VectorSpaceModelConfig.saveToFile(model, modelPath);
            System.out.printf("  Saved model: %s%n", modelPath.getFileName());

            // Create dataset.yaml
            String derivedName = datasetName != null ? datasetName :
                sourceDir.getFileName().toString() + "-derived";

            createDatasetYaml(targetDir, derivedName, sourceDataset, profile, derivedCount);
            System.out.printf("  Created dataset.yaml%n");

            // Print summary
            printSummary(sourceDir, targetDir, derivedName, dimensions, derivedCount, model);

            return 0;

        } catch (Exception e) {
            logger.error("Error deriving dataset", e);
            System.err.println("Error: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
            return 2;
        }
    }

    private void printHeader() {
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println("                         DERIVE MODEL-BASED DATASET                            ");
        System.out.println("═══════════════════════════════════════════════════════════════════════════════");
        System.out.println();
    }

    private BestFitSelector selectFitter(String type) {
        return switch (type.toLowerCase()) {
            case "bounded" -> BestFitSelector.boundedDataSelector();
            case "pearson" -> BestFitSelector.fullPearsonSelector();
            case "empirical" -> BestFitSelector.defaultSelector(); // Will use empirical fallback
            case "normalized" -> BestFitSelector.normalizedVectorSelector();
            default -> BestFitSelector.boundedDataWithEmpirical();
        };
    }

    private float[][] loadVectors(BaseVectors baseVectors, int count) {
        float[][] vectors = new float[count][];
        for (int i = 0; i < count; i++) {
            vectors[i] = baseVectors.get(i);
        }
        return vectors;
    }

    private void printModelSummary(VectorSpaceModel model) {
        Map<String, Integer> typeCounts = new LinkedHashMap<>();
        for (ScalarModel sm : model.scalarModels()) {
            String type = sm.getClass().getSimpleName().replace("ScalarModel", "");
            typeCounts.merge(type, 1, Integer::sum);
        }

        System.out.println();
        System.out.println("  Model distribution types:");
        for (Map.Entry<String, Integer> entry : typeCounts.entrySet()) {
            double pct = 100.0 * entry.getValue() / model.dimensions();
            System.out.printf("    %s: %d (%.1f%%)%n", entry.getKey(), entry.getValue(), pct);
        }
    }

    private void createDatasetYaml(Path targetDir, String name, TestDataGroup source,
                                   TestDataView profile, long vectorCount) throws IOException {

        Path yamlPath = targetDir.resolve("dataset.yaml");

        try (Writer writer = Files.newBufferedWriter(yamlPath)) {
            writer.write("# Derived dataset generated from: " + source.getName() + "\n");
            writer.write("# Profile: " + profile.getName() + "\n");
            writer.write("# Generated by: nbvectors generate derive\n");
            writer.write("\n");

            // Attributes section
            writer.write("attributes:\n");
            writer.write("  derived_from: " + source.getName() + "\n");
            writer.write("  generation_mode: virtdata\n");

            // Copy distance function if available
            Object distFunc = source.getAttribute("distance_function");
            if (distFunc != null) {
                writer.write("  distance_function: " + distFunc + "\n");
            }

            // Copy other relevant attributes
            Object license = source.getAttribute("license");
            if (license != null) {
                writer.write("  license: " + license + "\n");
            }

            Object vendor = source.getAttribute("vendor");
            if (vendor != null) {
                writer.write("  vendor: " + vendor + "\n");
            }

            writer.write("\n");

            // Profiles section
            writer.write("profiles:\n");
            writer.write("  default:\n");
            writer.write("    base_vectors:\n");
            writer.write("      source: model.json\n");
            writer.write("      window: 0.." + vectorCount + "\n");
        }
    }

    private void printSummary(Path sourceDir, Path targetDir, String name,
                              int dimensions, long vectorCount, VectorSpaceModel model) {
        System.out.println();
        System.out.println(ANSI_GREEN + "╔═══════════════════════════════════════════════════════════════════════════════╗" + ANSI_RESET);
        System.out.println(ANSI_GREEN + "║" + ANSI_GREEN + ANSI_BOLD + "                      ✓ DERIVED DATASET CREATED                                " + ANSI_RESET + ANSI_GREEN + "║" + ANSI_RESET);
        System.out.println(ANSI_GREEN + "╠═══════════════════════════════════════════════════════════════════════════════╣" + ANSI_RESET);
        System.out.printf(ANSI_GREEN + "║" + ANSI_RESET + "  Source:      %-60s" + ANSI_GREEN + "║%n" + ANSI_RESET, sourceDir.getFileName());
        System.out.printf(ANSI_GREEN + "║" + ANSI_RESET + "  Target:      %-60s" + ANSI_GREEN + "║%n" + ANSI_RESET, targetDir);
        System.out.printf(ANSI_GREEN + "║" + ANSI_RESET + "  Dimensions:  %-60d" + ANSI_GREEN + "║%n" + ANSI_RESET, dimensions);
        System.out.printf(ANSI_GREEN + "║" + ANSI_RESET + "  Vectors:     %-60s" + ANSI_GREEN + "║%n" + ANSI_RESET, String.format("%,d (generated on access)", vectorCount));
        System.out.println(ANSI_GREEN + "║" + ANSI_RESET + "                                                                               " + ANSI_GREEN + "║" + ANSI_RESET);
        System.out.println(ANSI_GREEN + "║" + ANSI_RESET + "  Files created:                                                              " + ANSI_GREEN + "║" + ANSI_RESET);
        System.out.println(ANSI_GREEN + "║" + ANSI_RESET + "    - model.json      (statistical model)                                     " + ANSI_GREEN + "║" + ANSI_RESET);
        System.out.println(ANSI_GREEN + "║" + ANSI_RESET + "    - dataset.yaml    (dataset configuration)                                 " + ANSI_GREEN + "║" + ANSI_RESET);
        System.out.println(ANSI_GREEN + "╚═══════════════════════════════════════════════════════════════════════════════╝" + ANSI_RESET);
        System.out.println();
        System.out.println("Usage:");
        System.out.printf("  # Load the derived dataset%n");
        System.out.printf("  nbvectors datasets list --catalog %s%n", targetDir);
        System.out.println();
    }

    private void deleteRecursively(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (var entries = Files.list(path)) {
                for (Path entry : entries.toList()) {
                    deleteRecursively(entry);
                }
            }
        }
        Files.deleteIfExists(path);
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new CMD_generate_derive()).execute(args);
        System.exit(exitCode);
    }
}
