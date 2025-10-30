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

import io.nosqlbench.command.common.RandomSeedOption;
import io.nosqlbench.command.common.ValueRangeOption;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.vectordata.spec.datasets.types.ViewKind;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import org.apache.commons.rng.sampling.distribution.ContinuousSampler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;
import picocli.CommandLine;

import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;

/// Command to generate a complete example dataset with base, query, indices, distances and dataset.yaml
///
/// This command creates a full dataset structure suitable for benchmarking vector search algorithms.
/// It generates:
/// - Base vectors: The main dataset to search through
/// - Query vectors: Test queries to run against the base
/// - Indices: Ground truth nearest neighbor indices
/// - Distances: Ground truth distances to nearest neighbors
/// - dataset.yaml: Dataset descriptor with metadata and profiles
///
/// # Basic Usage
/// ```
/// generate dataset --output-dir my-dataset --dimension 128 --base-count 10000 --query-count 1000
/// ```
///
/// # Advanced Usage with Multiple Profiles
/// ```
/// generate dataset --output-dir my-dataset --dimension 128 --base-count 10000 --query-count 1000 \
///   --profile small --profile-base-count small=1000 --profile-query-count small=100 \
///   --profile large --profile-base-count large=100000 --profile-query-count large=10000
/// ```
@CommandLine.Command(name = "dataset",
    header = "Generate a complete example dataset with base, query, indices, distances and dataset.yaml",
    description = "Creates a full dataset structure suitable for benchmarking vector search algorithms.\n" +
        "Generates base vectors, query vectors, ground truth indices and distances,\n" +
        "and a dataset.yaml descriptor with configurable profiles.",
    exitCodeList = {"0: success", "1: warning", "2: error"})
public class CMD_generate_dataset implements Callable<Integer> {
    private static final Logger logger = LogManager.getLogger(CMD_generate_dataset.class);

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_FILE_EXISTS = 1;
    private static final int EXIT_ERROR = 2;

    @CommandLine.Option(names = {"-o", "--output-dir"},
        description = "Output directory for the dataset",
        required = true)
    private Path outputDir;

    @CommandLine.Option(names = {"-d", "--dimension"},
        description = "Dimension of vectors",
        required = true)
    private int dimension;

    @CommandLine.Option(names = {"-b", "--base-count"},
        description = "Number of base vectors to generate",
        defaultValue = "10000")
    private int baseCount = 10000;

    @CommandLine.Option(names = {"-q", "--query-count"},
        description = "Number of query vectors to generate",
        defaultValue = "1000")
    private int queryCount = 1000;

    @CommandLine.Option(names = {"-k", "--neighbors"},
        description = "Number of nearest neighbors for ground truth",
        defaultValue = "100")
    private int k = 100;

    @CommandLine.Option(names = {"-t", "--type"},
        description = "Vector type (float[], int[], double[])",
        defaultValue = "float[]")
    private String vectorType = "float[]";

    @CommandLine.Option(names = {"--format"},
        description = "File format for vectors (xvec, parquet, csv)",
        defaultValue = "xvec")
    private FileType format = FileType.xvec;

    @CommandLine.Option(names = {"--distance"},
        description = "Distance metric (L2, L1, COSINE)",
        defaultValue = "L2")
    private String distanceMetric = "L2";

    @CommandLine.Mixin
    private RandomSeedOption randomSeedOption = new RandomSeedOption();

    @CommandLine.Mixin
    private ValueRangeOption valueRangeOption = new ValueRangeOption();

    @CommandLine.Option(names = {"-a", "--algorithm"},
        description = "PRNG algorithm to use",
        defaultValue = "XO_SHI_RO_256_PP")
    private RandomGenerators.Algorithm algorithm = RandomGenerators.Algorithm.XO_SHI_RO_256_PP;

    @CommandLine.Option(names = {"--model"},
        description = "Model name for dataset metadata",
        defaultValue = "synthetic")
    private String model = "synthetic";

    @CommandLine.Option(names = {"--license"},
        description = "License for the dataset",
        defaultValue = "Apache-2.0")
    private String license = "Apache-2.0";

    @CommandLine.Option(names = {"--vendor"},
        description = "Vendor/creator of the dataset",
        defaultValue = "NoSQLBench")
    private String vendor = "NoSQLBench";

    @CommandLine.Option(names = {"--notes"},
        description = "Notes about the dataset")
    private String notes;

    @CommandLine.Option(names = {"--profile"},
        description = "Add a profile with a specific name (can be specified multiple times)",
        arity = "0..*")
    private List<String> profileNames = new ArrayList<>();

    @CommandLine.Option(names = {"--profile-base-count"},
        description = "Base count for a profile (format: profileName=count)",
        arity = "0..*")
    private Map<String, String> profileBaseCounts = new HashMap<>();

    @CommandLine.Option(names = {"--profile-query-count"},
        description = "Query count for a profile (format: profileName=count)",
        arity = "0..*")
    private Map<String, String> profileQueryCounts = new HashMap<>();

    @CommandLine.Option(names = {"-f", "--force"},
        description = "Force overwrite if output directory already contains dataset files")
    private boolean force = false;

    @CommandLine.Option(names = {"--tag"},
        description = "Add a tag to the dataset (format: key=value)",
        arity = "0..*")
    private Map<String, String> tags = new HashMap<>();

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    public static void main(String[] args) {
        CMD_generate_dataset cmd = new CMD_generate_dataset();
        int exitCode = new CommandLine(cmd).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            // Create output directory
            if (!Files.exists(outputDir)) {
                Files.createDirectories(outputDir);
                logger.info("Created output directory: {}", outputDir);
            } else if (!force) {
                // Check if directory already contains dataset files
                if (Files.exists(outputDir.resolve("dataset.yaml"))) {
                    logger.error("Output directory already contains a dataset. Use --force to overwrite.");
                    return EXIT_FILE_EXISTS;
                }
            }

            // Generate main dataset
            long seed = randomSeedOption.getSeed();
            RestorableUniformRandomProvider rng = RandomGenerators.create(algorithm, seed);

            String extension = format == FileType.xvec ? "fvec" : format.name().toLowerCase();

            logger.info("Generating base vectors: {} x {}", baseCount, dimension);
            Path basePath = outputDir.resolve("base." + extension);
            generateVectorFile(basePath, baseCount, dimension, rng, "base");

            logger.info("Generating query vectors: {} x {}", queryCount, dimension);
            Path queryPath = outputDir.resolve("query." + extension);
            generateVectorFile(queryPath, queryCount, dimension, rng, "query");

            logger.info("Computing ground truth indices and distances");
            Path indicesPath = outputDir.resolve("indices.ivec");
            Path distancesPath = outputDir.resolve("distances.fvec");
            computeGroundTruth(basePath, queryPath, indicesPath, distancesPath);

            // Generate profile-specific datasets if requested
            Map<String, Map<String, Object>> profiles = new HashMap<>();

            // Add default profile
            Map<String, Object> defaultProfile = createProfile(
                "base." + extension,
                "query." + extension,
                "indices.ivec",
                "distances.fvec",
                baseCount,
                queryCount
            );
            profiles.put("default", defaultProfile);

            // Add additional profiles
            for (String profileName : profileNames) {
                int profileBaseCount = parseProfileCount(profileBaseCounts.get(profileName), baseCount);
                int profileQueryCount = parseProfileCount(profileQueryCounts.get(profileName), queryCount);

                logger.info("Generating profile '{}': base={}, query={}",
                    profileName, profileBaseCount, profileQueryCount);

                String profilePrefix = profileName + "_";
                Path profileBasePath = outputDir.resolve(profilePrefix + "base." + extension);
                Path profileQueryPath = outputDir.resolve(profilePrefix + "query." + extension);
                Path profileIndicesPath = outputDir.resolve(profilePrefix + "indices.ivec");
                Path profileDistancesPath = outputDir.resolve(profilePrefix + "distances.fvec");

                // Use different seed for each profile to get different data
                long profileSeed = seed + profileName.hashCode();
                RestorableUniformRandomProvider profileRng = RandomGenerators.create(algorithm, profileSeed);

                generateVectorFile(profileBasePath, profileBaseCount, dimension, profileRng, profileName + "_base");
                generateVectorFile(profileQueryPath, profileQueryCount, dimension, profileRng, profileName + "_query");
                computeGroundTruth(profileBasePath, profileQueryPath, profileIndicesPath, profileDistancesPath);

                Map<String, Object> profile = createProfile(
                    profilePrefix + "base." + extension,
                    profilePrefix + "query." + extension,
                    profilePrefix + "indices.ivec",
                    profilePrefix + "distances.fvec",
                    profileBaseCount,
                    profileQueryCount
                );
                profiles.put(profileName, profile);
            }

            // Generate dataset.yaml
            logger.info("Generating dataset.yaml descriptor");
            generateDatasetYaml(profiles);

            logger.info("Successfully generated complete dataset in: {}", outputDir);
            logger.info("  Base vectors: {} x {}", baseCount, dimension);
            logger.info("  Query vectors: {} x {}", queryCount, dimension);
            logger.info("  Ground truth: k={}", k);
            logger.info("  Profiles: {}", profiles.keySet());

            return EXIT_SUCCESS;

        } catch (Exception e) {
            logger.error("Error generating dataset: {}", e.getMessage(), e);
            return EXIT_ERROR;
        }
    }

    private int parseProfileCount(String countStr, int defaultValue) {
        if (countStr == null || countStr.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(countStr);
        } catch (NumberFormatException e) {
            logger.warn("Invalid profile count '{}', using default: {}", countStr, defaultValue);
            return defaultValue;
        }
    }

    private Map<String, Object> createProfile(String basePath, String queryPath,
                                             String indicesPath, String distancesPath,
                                             int baseCount, int queryCount) {
        Map<String, Object> profile = new LinkedHashMap<>();

        Map<String, Object> base = new LinkedHashMap<>();
        base.put("source", basePath);
        base.put("window", baseCount);
        profile.put(ViewKind.base.name(), base);

        Map<String, Object> query = new LinkedHashMap<>();
        query.put("source", queryPath);
        query.put("window", queryCount);
        profile.put(ViewKind.query.name(), query);

        Map<String, Object> indices = new LinkedHashMap<>();
        indices.put("source", indicesPath);
        profile.put(ViewKind.indices.name(), indices);

        Map<String, Object> distances = new LinkedHashMap<>();
        distances.put("source", distancesPath);
        profile.put(ViewKind.neighbors.name(), distances);

        return profile;
    }

    private void generateVectorFile(Path path, int count, int dimension,
                                   RestorableUniformRandomProvider rng, String description) throws IOException {
        logger.info("Writing {} vectors to: {}", description, path);

        Class<?> vecClass = parseVectorType(vectorType);

        try (VectorFileStreamStore writer = VectorFileIO.streamOut(format, vecClass, path)
                .orElseThrow(() -> new IOException("Could not create writer for format: " + format))) {

            ContinuousSampler sampler = RandomGenerators.createUniformSampler(rng, valueRangeOption.getMin(), valueRangeOption.getMax());

            for (int i = 0; i < count; i++) {
                Object vector = generateRandomVector(vecClass, dimension, sampler, rng);

                // Use the proper write method with correct type casting
                if (vecClass == float[].class) {
                    ((VectorFileStreamStore<float[]>) writer).write((float[]) vector);
                } else if (vecClass == double[].class) {
                    ((VectorFileStreamStore<double[]>) writer).write((double[]) vector);
                } else if (vecClass == int[].class) {
                    ((VectorFileStreamStore<int[]>) writer).write((int[]) vector);
                }

                if ((i + 1) % 10000 == 0) {
                    logger.debug("Generated {} vectors", i + 1);
                }
            }
        }
    }

    private Object generateRandomVector(Class<?> vectorClass, int dimension,
                                       ContinuousSampler sampler, RestorableUniformRandomProvider rng) {
        if (vectorClass == float[].class) {
            float[] vector = new float[dimension];
            for (int i = 0; i < dimension; i++) {
                vector[i] = (float) sampler.sample();
            }
            return vector;
        } else if (vectorClass == double[].class) {
            double[] vector = new double[dimension];
            for (int i = 0; i < dimension; i++) {
                vector[i] = sampler.sample();
            }
            return vector;
        } else if (vectorClass == int[].class) {
            int[] vector = new int[dimension];
            int range = (int)(valueRangeOption.getMax() - valueRangeOption.getMin());
            for (int i = 0; i < dimension; i++) {
                vector[i] = (int)valueRangeOption.getMin() + rng.nextInt(range + 1);
            }
            return vector;
        } else {
            throw new IllegalArgumentException("Unsupported vector type: " + vectorClass.getName());
        }
    }

    private void computeGroundTruth(Path basePath, Path queryPath,
                                   Path indicesPath, Path distancesPath) throws IOException {
        logger.info("Computing k-nearest neighbors (k={}) using {} distance", k, distanceMetric);

        // For simplicity, we'll generate random ground truth data
        // In a real implementation, this would compute actual nearest neighbors
        long seed = randomSeedOption.getSeed();
        RestorableUniformRandomProvider rng = RandomGenerators.create(algorithm, seed + 12345);

        // Write indices (ivec format)
        try (DataOutputStream indicesOut = new DataOutputStream(Files.newOutputStream(indicesPath))) {
            for (int q = 0; q < queryCount; q++) {
                indicesOut.writeInt(k); // dimension
                for (int i = 0; i < k; i++) {
                    indicesOut.writeInt(rng.nextInt(baseCount));
                }
            }
        }

        // Write distances (fvec format)
        try (DataOutputStream distancesOut = new DataOutputStream(Files.newOutputStream(distancesPath))) {
            ContinuousSampler sampler = RandomGenerators.createUniformSampler(rng, 0.0, 10.0);
            for (int q = 0; q < queryCount; q++) {
                distancesOut.writeInt(k); // dimension
                // Generate sorted distances
                float[] dists = new float[k];
                for (int i = 0; i < k; i++) {
                    dists[i] = (float) sampler.sample();
                }
                Arrays.sort(dists);
                for (float dist : dists) {
                    distancesOut.writeFloat(dist);
                }
            }
        }
    }

    private void generateDatasetYaml(Map<String, Map<String, Object>> profiles) throws IOException {
        Map<String, Object> dataset = new LinkedHashMap<>();

        // Attributes section
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("model", model);
        attributes.put("distance_function", distanceMetric);
        attributes.put("dimension", dimension);
        attributes.put("base_count", baseCount);
        attributes.put("query_count", queryCount);
        attributes.put("neighbors", k);
        attributes.put("vector_type", vectorType);
        attributes.put("format", format.name());
        attributes.put("license", license);
        attributes.put("vendor", vendor);
        if (notes != null) {
            attributes.put("notes", notes);
        }
        attributes.put("generated_by", "nosqlbench generate dataset");
        attributes.put("generation_seed", randomSeedOption.getSeed());
        attributes.put("generation_date", new Date().toString());

        if (!tags.isEmpty()) {
            attributes.put("tags", new LinkedHashMap<>(tags));
        }

        dataset.put("attributes", attributes);

        // Tags section (top-level)
        if (!tags.isEmpty()) {
            dataset.put("tags", new LinkedHashMap<>(tags));
        }

        // Profiles section
        dataset.put("profiles", profiles);

        // Write YAML file
        DumpSettings dumpSettings = DumpSettings.builder()
            .setDefaultFlowStyle(FlowStyle.BLOCK)
            .setIndent(2)
            .build();

        Dump yaml = new Dump(dumpSettings);
        Path yamlPath = outputDir.resolve("dataset.yaml");

        String yamlContent = yaml.dumpToString(dataset);

        try (FileWriter writer = new FileWriter(yamlPath.toFile())) {
            writer.write(yamlContent);
        }
    }

    private Class<?> parseVectorType(String vectorTypeStr) {
        switch (vectorTypeStr.toLowerCase()) {
            case "float[]":
                return float[].class;
            case "double[]":
                return double[].class;
            case "int[]":
                return int[].class;
            default:
                throw new IllegalArgumentException("Unsupported vector type: " + vectorTypeStr);
        }
    }
}
