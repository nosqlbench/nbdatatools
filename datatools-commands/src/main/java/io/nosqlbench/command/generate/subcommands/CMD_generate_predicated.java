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
import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.vectordata.discovery.metadata.FieldDescriptor;
import io.nosqlbench.vectordata.discovery.metadata.FieldType;
import io.nosqlbench.vectordata.discovery.metadata.MetadataLayoutImpl;
import io.nosqlbench.vectordata.discovery.metadata.slab.SlabtasticPredicateWriter;
import io.nosqlbench.vectordata.spec.predicates.ConjugateNode;
import io.nosqlbench.vectordata.spec.predicates.ConjugateType;
import io.nosqlbench.vectordata.spec.predicates.OpType;
import io.nosqlbench.vectordata.spec.predicates.PNode;
import io.nosqlbench.vectordata.spec.predicates.PredicateContext;
import io.nosqlbench.vectordata.spec.predicates.PredicateNode;
import org.apache.commons.rng.RestorableUniformRandomProvider;
import org.apache.commons.rng.sampling.distribution.ContinuousSampler;
import org.apache.commons.rng.sampling.distribution.GaussianSampler;
import org.apache.commons.rng.sampling.distribution.ZigguratNormalizedGaussianSampler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;
import picocli.CommandLine;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/// Command to generate a predicated demo dataset with metadata, predicates, and ground-truth results.
///
/// This command synthesizes a dataset with:
/// - Base and query float vectors (standard fvec format)
/// - Metadata records with `name` (TEXT) and `number` (INT) fields
/// - Random predicate queries (AND of 1–2 field comparisons)
/// - Ground-truth result indices computed via SQLite
///
/// All predicate-related data (metadata layout, metadata content, predicates, result indices) is
/// written to a single `.slab` file. A `dataset.yaml` descriptor ties everything together.
///
/// ## Usage
/// ```
/// generate predicated -o my-predicated-dataset -r 100000 -q 1000 -d 128
/// ```
@CommandLine.Command(name = "predicated",
    header = "Generate a predicated demo dataset with metadata, predicates, and ground-truth results",
    description = "Synthesizes metadata records, random predicate queries, and uses SQLite as a\n" +
        "ground-truth solver to compute which metadata records match each predicate.\n" +
        "Writes base/query vectors as fvec files and all predicate data to a single .slab file.",
    exitCodeList = {"0: success", "1: warning", "2: error"})
public class CMD_generate_predicated implements Callable<Integer> {

    /// Creates a new CMD_generate_predicated instance.
    public CMD_generate_predicated() {
    }

    private static final Logger logger = LogManager.getLogger(CMD_generate_predicated.class);

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_FILE_EXISTS = 1;
    private static final int EXIT_ERROR = 2;

    /// Name field index in the metadata layout
    private static final int FIELD_NAME = 0;
    /// Number field index in the metadata layout
    private static final int FIELD_NUMBER = 1;

    /// Operators usable for numeric comparisons on the `number` field
    private static final OpType[] NUMBER_OPS = {OpType.GT, OpType.LT, OpType.GE, OpType.LE, OpType.EQ};

    @CommandLine.Option(names = {"-o", "--output-dir"},
        description = "Output directory for the dataset",
        required = true)
    private Path outputDir;

    @CommandLine.Option(names = {"-r", "--record-count"},
        description = "Number of metadata records to generate",
        defaultValue = "1000000")
    private int recordCount = 1_000_000;

    @CommandLine.Option(names = {"-q", "--query-count"},
        description = "Number of predicate queries to generate",
        defaultValue = "10000")
    private int queryCount = 10_000;

    @CommandLine.Option(names = {"-d", "--dimension"},
        description = "Vector dimension",
        defaultValue = "256")
    private int dimension = 256;

    @CommandLine.Mixin
    private RandomSeedOption randomSeedOption = new RandomSeedOption();

    @CommandLine.Option(names = {"-k", "--limit"},
        description = "Maximum number of result indices per query. " +
            "When unset, all matching records are returned for each predicate.",
        defaultValue = "0")
    private int limit = 0;

    @CommandLine.Option(names = {"-f", "--force"},
        description = "Force overwrite if output directory already contains dataset files")
    private boolean force = false;

    @CommandLine.Option(names = {"-a", "--algorithm"},
        description = "PRNG algorithm to use",
        defaultValue = "XO_SHI_RO_256_PP")
    private RandomGenerators.Algorithm algorithm = RandomGenerators.Algorithm.XO_SHI_RO_256_PP;

    @CommandLine.Option(names = {"--named-fields"},
        description = "Use named fields in predicates instead of positional indices. " +
            "Named predicates are self-describing and not coupled to a specific metadata layout.")
    private boolean namedFields = false;

    @Override
    public Integer call() {
        try {
            if (!Files.exists(outputDir)) {
                Files.createDirectories(outputDir);
                logger.info("Created output directory: {}", outputDir);
            } else if (!force && Files.exists(outputDir.resolve("dataset.yaml"))) {
                logger.error("Output directory already contains a dataset. Use --force to overwrite.");
                return EXIT_FILE_EXISTS;
            }

            long seed = randomSeedOption.getSeed();
            RestorableUniformRandomProvider rng = RandomGenerators.create(algorithm, seed);

            MetadataLayoutImpl layout = new MetadataLayoutImpl(List.of(
                new FieldDescriptor("name", FieldType.TEXT),
                new FieldDescriptor("number", FieldType.INT)
            ));

            // Build the string table: all possible name strings mapped to ordinals
            Map<String, Long> nameToOrdinal = new HashMap<>();
            Map<Long, String> ordinalToName = new HashMap<>();
            for (int i = 1; i <= 100; i++) {
                String name = NumberNames.toName(i);
                long ordinal = i;
                nameToOrdinal.put(name, ordinal);
                ordinalToName.put(ordinal, name);
            }

            Path slabPath = outputDir.resolve("predicates.slab");
            Path dbPath = outputDir.resolve("temp_solver.db");

            try (
                SlabtasticPredicateWriter slabWriter = new SlabtasticPredicateWriter(slabPath, layout);
                Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toAbsolutePath())
            ) {
                configureConnection(conn);
                createMetadataTable(conn);

                // --- Phase 1: Generate metadata records ---
                logger.info("Generating {} metadata records...", recordCount);
                ContinuousSampler nameSampler = GaussianSampler.of(
                    ZigguratNormalizedGaussianSampler.of(rng), 50.0, 10.0);

                try (PreparedStatement insert = conn.prepareStatement(
                    "INSERT INTO metadata (ordinal, name, number) VALUES (?, ?, ?)")) {
                    conn.setAutoCommit(false);
                    for (int i = 0; i < recordCount; i++) {
                        int nameNum = clamp((int) Math.round(nameSampler.sample()), 1, 100);
                        String name = NumberNames.toName(nameNum);
                        int number = 1 + rng.nextInt(100);

                        insert.setInt(1, i);
                        insert.setString(2, name);
                        insert.setInt(3, number);
                        insert.addBatch();

                        slabWriter.writeMetadataRecord(i, Map.of("name", name, "number", (long) number));

                        if ((i + 1) % 100_000 == 0) {
                            insert.executeBatch();
                            conn.commit();
                            logger.info("  wrote {} metadata records", i + 1);
                        }
                    }
                    insert.executeBatch();
                    conn.commit();
                    conn.setAutoCommit(true);
                }
                logger.info("Metadata records complete.");

                // Create index for faster query solving
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE INDEX idx_name ON metadata (name)");
                    stmt.execute("CREATE INDEX idx_number ON metadata (number)");
                }

                // --- Phase 2: Generate predicates and solve via SQLite ---
                logger.info("Generating {} predicate queries and solving via SQLite...", queryCount);
                PredicateContext ctx = namedFields
                    ? PredicateContext.named(layout)
                    : PredicateContext.indexed(layout);
                for (int q = 0; q < queryCount; q++) {
                    PNode<?> predicate = generateRandomPredicate(rng, nameToOrdinal);
                    String whereClause = namedFields
                        ? PNodeToSql.toSql(predicate, ctx, ordinalToName)
                        : PNodeToSql.toSql(predicate, layout, ordinalToName);

                    int[] resultIndices = solveQuery(conn, whereClause, limit);

                    slabWriter.writePredicate(q, predicate);
                    slabWriter.writeResultIndices(q, resultIndices);

                    if ((q + 1) % 1000 == 0) {
                        logger.info("  solved {} / {} predicates", q + 1, queryCount);
                    }
                }
                logger.info("Predicate solving complete.");
            }

            // Clean up temp database
            Files.deleteIfExists(dbPath);

            // --- Phase 3: Generate base and query vectors ---
            logger.info("Generating base vectors: {} x {}", recordCount, dimension);
            Path basePath = outputDir.resolve("base.fvec");
            generateVectorFile(basePath, recordCount, rng);

            logger.info("Generating query vectors: {} x {}", queryCount, dimension);
            Path queryPath = outputDir.resolve("query.fvec");
            generateVectorFile(queryPath, queryCount, rng);

            // --- Phase 4: Write dataset.yaml ---
            logger.info("Writing dataset.yaml");
            generateDatasetYaml();

            logger.info("Successfully generated predicated dataset in: {}", outputDir);
            logger.info("  Records: {}", recordCount);
            logger.info("  Predicates: {}", queryCount);
            logger.info("  Dimension: {}", dimension);
            return EXIT_SUCCESS;

        } catch (Exception e) {
            logger.error("Error generating predicated dataset: {}", e.getMessage(), e);
            return EXIT_ERROR;
        }
    }

    /// Configures the SQLite connection for bulk write performance.
    ///
    /// @param conn the JDBC connection
    /// @throws SQLException if a PRAGMA statement fails
    private void configureConnection(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA journal_mode=WAL");
            stmt.execute("PRAGMA synchronous=OFF");
            stmt.execute("PRAGMA cache_size=-200000");
        }
    }

    /// Creates the metadata table in the SQLite database.
    ///
    /// @param conn the JDBC connection
    /// @throws SQLException if the CREATE TABLE statement fails
    private void createMetadataTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE metadata (ordinal INTEGER PRIMARY KEY, name TEXT, number INTEGER)");
        }
    }

    /// Generates a random predicate: either a single predicate or an AND of two predicates.
    ///
    /// @param rng            the random number generator
    /// @param nameToOrdinal  mapping from name strings to their long ordinals
    /// @return a random predicate tree
    private PNode<?> generateRandomPredicate(RestorableUniformRandomProvider rng,
                                             Map<String, Long> nameToOrdinal) {
        int predicateCount = 1 + rng.nextInt(2); // 1 or 2 predicates
        if (predicateCount == 1) {
            return generateSinglePredicate(rng, nameToOrdinal);
        }
        PNode<?> p1 = generateSinglePredicate(rng, nameToOrdinal);
        PNode<?> p2 = generateSinglePredicate(rng, nameToOrdinal);
        return new ConjugateNode(ConjugateType.AND, p1, p2);
    }

    /// Generates a single leaf predicate on either the `name` or `number` field.
    ///
    /// When {@code --named-fields} is set, predicates use field names instead of
    /// positional indices, making them self-describing.
    ///
    /// @param rng            the random number generator
    /// @param nameToOrdinal  mapping from name strings to their long ordinals
    /// @return a single predicate node
    private PNode<?> generateSinglePredicate(RestorableUniformRandomProvider rng,
                                             Map<String, Long> nameToOrdinal) {
        boolean useNameField = rng.nextBoolean();
        if (useNameField) {
            int nameNum = 1 + rng.nextInt(100);
            String name = NumberNames.toName(nameNum);
            long ordinal = nameToOrdinal.get(name);
            if (namedFields) {
                return new PredicateNode("name", OpType.EQ, ordinal);
            }
            return new PredicateNode(FIELD_NAME, OpType.EQ, ordinal);
        } else {
            OpType op = NUMBER_OPS[rng.nextInt(NUMBER_OPS.length)];
            long threshold = 1 + rng.nextInt(100);
            if (namedFields) {
                return new PredicateNode("number", op, threshold);
            }
            return new PredicateNode(FIELD_NUMBER, op, threshold);
        }
    }

    /// Executes a predicate query against the SQLite database and returns matching ordinals.
    ///
    /// @param conn        the JDBC connection
    /// @param whereClause the SQL WHERE clause
    /// @param limit       maximum number of results to return (0 means unlimited)
    /// @return sorted array of matching record ordinals
    /// @throws SQLException if the query fails
    private int[] solveQuery(Connection conn, String whereClause, int limit) throws SQLException {
        String sql = "SELECT ordinal FROM metadata WHERE " + whereClause + " ORDER BY ordinal";
        if (limit > 0) {
            sql += " LIMIT " + limit;
        }
        List<Integer> results = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                results.add(rs.getInt(1));
            }
        }
        int[] arr = new int[results.size()];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = results.get(i);
        }
        return arr;
    }

    /// Generates a vector file in fvec format with normal(0,1) distributed components.
    ///
    /// @param path  the output file path
    /// @param count the number of vectors to generate
    /// @param rng   the random number generator
    /// @throws IOException if writing fails
    @SuppressWarnings("unchecked")
    private void generateVectorFile(Path path, int count,
                                    RestorableUniformRandomProvider rng) throws IOException {
        ContinuousSampler sampler = GaussianSampler.of(
            ZigguratNormalizedGaussianSampler.of(rng), 0.0, 1.0);

        try (VectorFileStreamStore<float[]> writer =
                 (VectorFileStreamStore<float[]>) VectorFileIO.streamOut(FileType.xvec, float[].class, path)
                     .orElseThrow(() -> new IOException("Could not create fvec writer for: " + path))) {
            for (int i = 0; i < count; i++) {
                float[] vec = new float[dimension];
                for (int j = 0; j < dimension; j++) {
                    vec[j] = (float) sampler.sample();
                }
                writer.write(vec);
                if ((i + 1) % 100_000 == 0) {
                    logger.debug("  wrote {} vectors to {}", i + 1, path.getFileName());
                }
            }
        }
    }

    /// Writes the dataset.yaml descriptor file.
    ///
    /// @throws IOException if writing fails
    private void generateDatasetYaml() throws IOException {
        Map<String, Object> dataset = new LinkedHashMap<>();

        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("model", "synthetic-predicated");
        attributes.put("distance_function", "L2");
        attributes.put("dimension", dimension);
        attributes.put("record_count", recordCount);
        attributes.put("query_count", queryCount);
        attributes.put("license", "Apache-2.0");
        attributes.put("vendor", "NoSQLBench");
        attributes.put("generated_by", "nosqlbench generate predicated");
        attributes.put("generation_seed", randomSeedOption.getSeed());
        dataset.put("attributes", attributes);

        Map<String, Object> defaultProfile = new LinkedHashMap<>();

        Map<String, Object> base = new LinkedHashMap<>();
        base.put("source", "base.fvec");
        defaultProfile.put("base", base);

        Map<String, Object> query = new LinkedHashMap<>();
        query.put("source", "query.fvec");
        defaultProfile.put("query", query);

        Map<String, Object> predicates = new LinkedHashMap<>();
        predicates.put("source", "predicates.slab");
        defaultProfile.put("metadata_predicates", predicates);

        Map<String, Object> results = new LinkedHashMap<>();
        results.put("source", "predicates.slab");
        defaultProfile.put("predicate_results", results);

        Map<String, Object> metadataLayout = new LinkedHashMap<>();
        metadataLayout.put("source", "predicates.slab");
        defaultProfile.put("metadata_layout", metadataLayout);

        Map<String, Object> metadataContent = new LinkedHashMap<>();
        metadataContent.put("source", "predicates.slab");
        defaultProfile.put("metadata_content", metadataContent);

        Map<String, Object> profiles = new LinkedHashMap<>();
        profiles.put("default", defaultProfile);
        dataset.put("profiles", profiles);

        DumpSettings dumpSettings = DumpSettings.builder()
            .setDefaultFlowStyle(FlowStyle.BLOCK)
            .setIndent(2)
            .build();
        Dump yaml = new Dump(dumpSettings);
        Path yamlPath = outputDir.resolve("dataset.yaml");
        try (FileWriter writer = new FileWriter(yamlPath.toFile())) {
            writer.write(yaml.dumpToString(dataset));
        }
    }

    /// Clamps a value to the given range.
    ///
    /// @param value the value to clamp
    /// @param min   the minimum (inclusive)
    /// @param max   the maximum (inclusive)
    /// @return the clamped value
    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }
}
