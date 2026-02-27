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

package io.nosqlbench.vectordata.benchmarks;

import io.nosqlbench.nbdatatools.api.fileio.VectorFileStreamStore;
import io.nosqlbench.nbdatatools.api.services.FileType;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.vectordata.discovery.metadata.FieldDescriptor;
import io.nosqlbench.vectordata.discovery.metadata.FieldType;
import io.nosqlbench.vectordata.discovery.metadata.MetadataLayoutImpl;
import io.nosqlbench.vectordata.discovery.metadata.MetadataRecordCodec;
import io.nosqlbench.vectordata.discovery.metadata.slab.SlabtasticPredicateBackend;
import io.nosqlbench.vectordata.discovery.metadata.slab.SlabtasticPredicateWriter;
import io.nosqlbench.vectordata.discovery.metadata.sqlite.SQLitePredicateWriter;
import io.nosqlbench.vectordata.spec.predicates.ConjugateNode;
import io.nosqlbench.vectordata.spec.predicates.ConjugateType;
import io.nosqlbench.vectordata.spec.predicates.OpType;
import io.nosqlbench.vectordata.spec.predicates.PNode;
import io.nosqlbench.vectordata.spec.predicates.PredicateContext;
import io.nosqlbench.vectordata.spec.predicates.PredicateNode;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

/// Shared utility for generating benchmark datasets.
///
/// Generates slab-backed and SQLite-backed predicated datasets with metadata
/// records, random predicate queries, and ground-truth results computed via
/// SQLite. Generated datasets are cached in `target/benchmark-data/` so they
/// survive `mvn test` reruns but are cleaned by `mvn clean`.
///
/// This class directly uses the low-level writing APIs from `datatools-slabtastic`
/// and `datatools-vectordata` to avoid a cyclic dependency on `datatools-commands`.
public final class BenchmarkDataGenerator {

    private BenchmarkDataGenerator() {
    }

    /// Operators usable for numeric comparisons on the `number` field
    private static final OpType[] NUMBER_OPS = {OpType.GT, OpType.LT, OpType.GE, OpType.LE, OpType.EQ};

    /// Ensures a slab-backed dataset exists at the given directory.
    ///
    /// If `dataset.yaml` already exists in the directory, this method returns
    /// immediately. Otherwise, it generates the dataset with base/query vectors,
    /// metadata records, predicates, and ground-truth results.
    ///
    /// @param dir       the output directory for the dataset
    /// @param records   the number of metadata records to generate
    /// @param queries   the number of predicate queries to generate
    /// @param dimension the vector dimension
    /// @param seed      the random seed for reproducibility
    /// @throws RuntimeException if data generation fails
    public static void ensureSlabDataset(Path dir, int records, int queries, int dimension, long seed) {
        if (Files.exists(dir.resolve("dataset.yaml"))) {
            System.out.println("[BenchmarkDataGenerator] Slab dataset already exists at " + dir);
            return;
        }

        System.out.println("[BenchmarkDataGenerator] Generating slab dataset: "
            + records + " records, " + queries + " queries, " + dimension + "d at " + dir);
        long start = System.currentTimeMillis();

        try {
            Files.createDirectories(dir);
            Random rng = new Random(seed);

            MetadataLayoutImpl layout = new MetadataLayoutImpl(List.of(
                new FieldDescriptor("name", FieldType.TEXT),
                new FieldDescriptor("number", FieldType.INT)
            ));

            Path slabPath = dir.resolve("predicates.slab");
            Path dbPath = dir.resolve("temp_solver.db");

            try (
                SlabtasticPredicateWriter slabWriter = new SlabtasticPredicateWriter(slabPath, layout);
                Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toAbsolutePath())
            ) {
                configureSqliteConnection(conn);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE metadata (ordinal INTEGER PRIMARY KEY, name TEXT, number INTEGER)");
                }

                // Phase 1: Generate metadata records
                System.out.println("[BenchmarkDataGenerator]   Generating " + records + " metadata records...");
                try (PreparedStatement insert = conn.prepareStatement(
                    "INSERT INTO metadata (ordinal, name, number) VALUES (?, ?, ?)")) {
                    conn.setAutoCommit(false);
                    for (int i = 0; i < records; i++) {
                        int nameNum = 1 + rng.nextInt(100);
                        String name = "name_" + nameNum;
                        int number = 1 + rng.nextInt(100);

                        insert.setInt(1, i);
                        insert.setString(2, name);
                        insert.setInt(3, number);
                        insert.addBatch();

                        slabWriter.writeMetadataRecord(i, Map.of("name", name, "number", (long) number));

                        if ((i + 1) % 50_000 == 0) {
                            insert.executeBatch();
                            conn.commit();
                            System.out.println("[BenchmarkDataGenerator]     wrote " + (i + 1) + " metadata records");
                        }
                    }
                    insert.executeBatch();
                    conn.commit();
                    conn.setAutoCommit(true);
                }

                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE INDEX idx_name ON metadata (name)");
                    stmt.execute("CREATE INDEX idx_number ON metadata (number)");
                }

                // Phase 2: Generate predicates and solve via SQLite
                System.out.println("[BenchmarkDataGenerator]   Generating " + queries + " predicates...");
                for (int q = 0; q < queries; q++) {
                    PNode<?> predicate = generateRandomPredicate(rng);
                    String whereClause = predicateToSql(predicate);
                    int[] resultIndices = solveQuery(conn, whereClause);

                    slabWriter.writePredicate(q, predicate);
                    slabWriter.writeResultIndices(q, resultIndices);

                    if ((q + 1) % 500 == 0) {
                        System.out.println("[BenchmarkDataGenerator]     solved " + (q + 1) + " / " + queries + " predicates");
                    }
                }
            }

            // Clean up temp database
            Files.deleteIfExists(dbPath);

            // Phase 3: Generate vector files
            System.out.println("[BenchmarkDataGenerator]   Generating vector files...");
            generateVectorFile(dir.resolve("base.fvec"), records, dimension, rng);
            generateVectorFile(dir.resolve("query.fvec"), queries, dimension, rng);

            // Phase 4: Write dataset.yaml
            writeSlabDatasetYaml(dir, dimension, records, queries, seed);

            long elapsed = System.currentTimeMillis() - start;
            System.out.println("[BenchmarkDataGenerator] Slab dataset generated in " + elapsed + " ms");

        } catch (Exception e) {
            throw new RuntimeException("Failed to generate slab dataset at " + dir, e);
        }
    }

    /// Ensures a SQLite-backed dataset exists at the given directory.
    ///
    /// First ensures a slab dataset exists (to share base/query vectors),
    /// then converts the slab predicate data to a SQLite database.
    ///
    /// @param dir       the output directory for the SQLite dataset
    /// @param slabDir   the slab dataset directory to convert from
    /// @param records   the number of metadata records
    /// @param queries   the number of predicate queries
    /// @param dimension the vector dimension
    /// @param seed      the random seed for reproducibility
    /// @throws RuntimeException if data generation or conversion fails
    public static void ensureSqliteDataset(Path dir, Path slabDir, int records, int queries,
                                           int dimension, long seed) {
        if (Files.exists(dir.resolve("dataset.yaml"))) {
            System.out.println("[BenchmarkDataGenerator] SQLite dataset already exists at " + dir);
            return;
        }

        ensureSlabDataset(slabDir, records, queries, dimension, seed);

        System.out.println("[BenchmarkDataGenerator] Converting slab data to SQLite at " + dir);
        long start = System.currentTimeMillis();

        try {
            Files.createDirectories(dir);

            linkOrCopy(slabDir.resolve("base.fvec"), dir.resolve("base.fvec"));
            linkOrCopy(slabDir.resolve("query.fvec"), dir.resolve("query.fvec"));

            Path slabPath = slabDir.resolve("predicates.slab");
            Path dbPath = dir.resolve("predicates.db");

            try (SlabtasticPredicateBackend slabBackend = new SlabtasticPredicateBackend(slabPath)) {
                Optional<ByteBuffer> layoutBuf = slabBackend.getMetadataLayout();
                if (layoutBuf.isEmpty()) {
                    throw new RuntimeException("No metadata layout found in slab file");
                }
                MetadataLayoutImpl layout = MetadataLayoutImpl.fromBuffer(layoutBuf.get());
                PredicateContext ctx = PredicateContext.indexed(layout);

                try (SQLitePredicateWriter sqlWriter = new SQLitePredicateWriter(dbPath, layout)) {
                    long contentCount = slabBackend.getMetadataContentCount();
                    for (long i = 0; i < contentCount; i++) {
                        Optional<ByteBuffer> buf = slabBackend.getMetadataContent(i);
                        if (buf.isPresent()) {
                            Map<String, Object> record = MetadataRecordCodec.decode(layout, buf.get());
                            sqlWriter.writeMetadataRecord(i, record);
                        }
                    }

                    long predCount = slabBackend.getPredicateCount();
                    for (long i = 0; i < predCount; i++) {
                        Optional<ByteBuffer> buf = slabBackend.getPredicate(i);
                        if (buf.isPresent()) {
                            PNode<?> pred = ctx.decode(buf.get());
                            sqlWriter.writePredicate(i, pred);
                        }
                    }

                    long riCount = slabBackend.getResultIndicesCount();
                    for (long i = 0; i < riCount; i++) {
                        Optional<ByteBuffer> buf = slabBackend.getResultIndices(i);
                        if (buf.isPresent()) {
                            int[] indices = decodeResultIndices(buf.get());
                            sqlWriter.writeResultIndices(i, indices);
                        }
                    }

                    sqlWriter.commit();
                }
            }

            writeSqliteDatasetYaml(dir, dimension, records, queries, seed);

            long elapsed = System.currentTimeMillis() - start;
            System.out.println("[BenchmarkDataGenerator] SQLite dataset generated in " + elapsed + " ms");

        } catch (Exception e) {
            throw new RuntimeException("Failed to generate SQLite dataset", e);
        }
    }

    /// Generates a random predicate: either a single predicate or an AND of two predicates.
    ///
    /// @param rng the random number generator
    /// @return a random predicate tree
    private static PNode<?> generateRandomPredicate(Random rng) {
        int count = 1 + rng.nextInt(2);
        if (count == 1) {
            return generateSinglePredicate(rng);
        }
        PNode<?> p1 = generateSinglePredicate(rng);
        PNode<?> p2 = generateSinglePredicate(rng);
        return new ConjugateNode(ConjugateType.AND, p1, p2);
    }

    /// Generates a single leaf predicate on either the `name` or `number` field.
    ///
    /// Uses indexed field representation (field index 0 = name, 1 = number).
    ///
    /// @param rng the random number generator
    /// @return a single predicate node
    private static PNode<?> generateSinglePredicate(Random rng) {
        if (rng.nextBoolean()) {
            int nameNum = 1 + rng.nextInt(100);
            return new PredicateNode(0, OpType.EQ, nameNum);
        } else {
            OpType op = NUMBER_OPS[rng.nextInt(NUMBER_OPS.length)];
            long threshold = 1 + rng.nextInt(100);
            return new PredicateNode(1, op, threshold);
        }
    }

    /// Converts a predicate tree to a SQL WHERE clause.
    ///
    /// @param node the predicate node
    /// @return the SQL WHERE clause string
    private static String predicateToSql(PNode<?> node) {
        if (node instanceof ConjugateNode conj) {
            StringBuilder sb = new StringBuilder("(");
            PNode<?>[] children = conj.values();
            for (int i = 0; i < children.length; i++) {
                if (i > 0) {
                    sb.append(conj.type() == ConjugateType.AND ? " AND " : " OR ");
                }
                sb.append(predicateToSql(children[i]));
            }
            return sb.append(")").toString();
        } else if (node instanceof PredicateNode pred) {
            String fieldName = pred.field() == 0 ? "name" : "number";
            String sqlOp = switch (pred.op()) {
                case EQ -> "=";
                case GT -> ">";
                case LT -> "<";
                case GE -> ">=";
                case LE -> "<=";
                default -> throw new IllegalArgumentException("Unsupported op: " + pred.op());
            };
            long value = pred.v()[0];
            if (pred.field() == 0) {
                return fieldName + " " + sqlOp + " 'name_" + value + "'";
            }
            return fieldName + " " + sqlOp + " " + value;
        }
        throw new IllegalArgumentException("Unknown predicate node type: " + node.getClass());
    }

    /// Executes a predicate query against the SQLite database.
    ///
    /// @param conn        the JDBC connection
    /// @param whereClause the SQL WHERE clause
    /// @return sorted array of matching record ordinals
    /// @throws SQLException if the query fails
    private static int[] solveQuery(Connection conn, String whereClause) throws SQLException {
        String sql = "SELECT ordinal FROM metadata WHERE " + whereClause + " ORDER BY ordinal";
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

    /// Generates a vector file in fvec format with random float components.
    ///
    /// @param path      the output file path
    /// @param count     the number of vectors to generate
    /// @param dimension the vector dimension
    /// @param rng       the random number generator
    /// @throws IOException if writing fails
    @SuppressWarnings("unchecked")
    private static void generateVectorFile(Path path, int count, int dimension, Random rng) throws IOException {
        try (VectorFileStreamStore<float[]> writer =
                 (VectorFileStreamStore<float[]>) VectorFileIO.streamOut(FileType.xvec, float[].class, path)
                     .orElseThrow(() -> new IOException("Could not create fvec writer for: " + path))) {
            for (int i = 0; i < count; i++) {
                float[] vec = new float[dimension];
                for (int j = 0; j < dimension; j++) {
                    vec[j] = (float) rng.nextGaussian();
                }
                writer.write(vec);
            }
        }
    }

    /// Configures a SQLite connection for bulk write performance.
    ///
    /// @param conn the JDBC connection
    /// @throws SQLException if a PRAGMA statement fails
    private static void configureSqliteConnection(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA journal_mode=WAL");
            stmt.execute("PRAGMA synchronous=OFF");
            stmt.execute("PRAGMA cache_size=-200000");
        }
    }

    /// Decodes result indices from a slab buffer.
    ///
    /// Wire format: `[count:4][index0:4][index1:4]...` in little-endian byte order.
    ///
    /// @param buf the buffer containing the encoded indices
    /// @return an array of matching record ordinals
    private static int[] decodeResultIndices(ByteBuffer buf) {
        buf = buf.order(ByteOrder.LITTLE_ENDIAN);
        int count = buf.getInt();
        int[] indices = new int[count];
        for (int i = 0; i < count; i++) {
            indices[i] = buf.getInt();
        }
        return indices;
    }

    /// Creates a symbolic link or copies a file if linking fails.
    ///
    /// @param source the source file path
    /// @param target the target file path
    /// @throws IOException if both linking and copying fail
    private static void linkOrCopy(Path source, Path target) throws IOException {
        if (Files.exists(target)) {
            return;
        }
        try {
            Files.createSymbolicLink(target, source.toAbsolutePath());
        } catch (IOException | UnsupportedOperationException e) {
            Files.copy(source, target);
        }
    }

    /// Writes a dataset.yaml for a slab-backed dataset.
    ///
    /// @param dir       the output directory
    /// @param dimension the vector dimension
    /// @param records   the record count
    /// @param queries   the query count
    /// @param seed      the generation seed
    /// @throws IOException if writing fails
    private static void writeSlabDatasetYaml(Path dir, int dimension, int records,
                                             int queries, long seed) throws IOException {
        writeDatasetYaml(dir, dimension, records, queries, seed,
            "predicates.slab", "nosqlbench benchmark data generator");
    }

    /// Writes a dataset.yaml for a SQLite-backed dataset.
    ///
    /// @param dir       the output directory
    /// @param dimension the vector dimension
    /// @param records   the record count
    /// @param queries   the query count
    /// @param seed      the generation seed
    /// @throws IOException if writing fails
    private static void writeSqliteDatasetYaml(Path dir, int dimension, int records,
                                               int queries, long seed) throws IOException {
        writeDatasetYaml(dir, dimension, records, queries, seed,
            "predicates.db", "nosqlbench benchmark data generator (sqlite conversion)");
    }

    /// Writes a dataset.yaml descriptor file.
    ///
    /// @param dir            the output directory
    /// @param dimension      the vector dimension
    /// @param records        the record count
    /// @param queries        the query count
    /// @param seed           the generation seed
    /// @param predicateFile  the predicate storage file name
    /// @param generatedBy    the generator description
    /// @throws IOException if writing fails
    private static void writeDatasetYaml(Path dir, int dimension, int records, int queries,
                                         long seed, String predicateFile, String generatedBy)
        throws IOException {
        Map<String, Object> dataset = new LinkedHashMap<>();

        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("model", "synthetic-predicated");
        attributes.put("distance_function", "L2");
        attributes.put("dimension", dimension);
        attributes.put("record_count", records);
        attributes.put("query_count", queries);
        attributes.put("license", "Apache-2.0");
        attributes.put("vendor", "NoSQLBench");
        attributes.put("generated_by", generatedBy);
        attributes.put("generation_seed", seed);
        dataset.put("attributes", attributes);

        Map<String, Object> defaultProfile = new LinkedHashMap<>();

        Map<String, Object> base = new LinkedHashMap<>();
        base.put("source", "base.fvec");
        defaultProfile.put("base", base);

        Map<String, Object> query = new LinkedHashMap<>();
        query.put("source", "query.fvec");
        defaultProfile.put("query", query);

        Map<String, Object> predicates = new LinkedHashMap<>();
        predicates.put("source", predicateFile);
        defaultProfile.put("metadata_predicates", predicates);

        Map<String, Object> results = new LinkedHashMap<>();
        results.put("source", predicateFile);
        defaultProfile.put("predicate_results", results);

        Map<String, Object> metadataLayout = new LinkedHashMap<>();
        metadataLayout.put("source", predicateFile);
        defaultProfile.put("metadata_layout", metadataLayout);

        Map<String, Object> metadataContent = new LinkedHashMap<>();
        metadataContent.put("source", predicateFile);
        defaultProfile.put("metadata_content", metadataContent);

        Map<String, Object> profiles = new LinkedHashMap<>();
        profiles.put("default", defaultProfile);
        dataset.put("profiles", profiles);

        DumpSettings dumpSettings = DumpSettings.builder()
            .setDefaultFlowStyle(FlowStyle.BLOCK)
            .setIndent(2)
            .build();
        Dump yaml = new Dump(dumpSettings);
        Path yamlPath = dir.resolve("dataset.yaml");
        try (FileWriter writer = new FileWriter(yamlPath.toFile())) {
            writer.write(yaml.dumpToString(dataset));
        }
    }
}
