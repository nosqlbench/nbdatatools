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

import io.nosqlbench.vectordata.VectorTestData;
import io.nosqlbench.vectordata.discovery.TestDataGroup;
import io.nosqlbench.vectordata.discovery.metadata.MetadataContent;
import io.nosqlbench.vectordata.discovery.metadata.MetadataLayout;
import io.nosqlbench.vectordata.discovery.metadata.PredicateTestDataView;
import io.nosqlbench.vectordata.discovery.metadata.Predicates;
import io.nosqlbench.vectordata.discovery.metadata.ResultIndices;
import io.nosqlbench.vectordata.spec.predicates.PNode;
import io.nosqlbench.vectordata.spec.predicates.PredicateContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/// JMH benchmarks for bulk predicate data access through {@link PredicateTestDataView}.
///
/// Exercises both slab and SQLite storage backends with identical logical data to
/// enable direct comparison of I/O characteristics. Data is generated once and
/// cached in `target/benchmark-data/`.
///
/// ## Running via exec:exec
///
/// ```bash
/// # Show all options
/// mvn exec:exec@bench-predicate-bulk -pl datatools-vectordata -Dbench.args="--help"
///
/// # SQLite only, quick run
/// mvn exec:exec@bench-predicate-bulk -pl datatools-vectordata \
///   -Dbench.args="--backend sqlite --batch-size 1000 --warmup 1 --iterations 1"
///
/// # Both backends, large dataset
/// mvn exec:exec@bench-predicate-bulk -pl datatools-vectordata \
///   -Dbench.args="--records 500000"
/// ```
///
/// After the JMH run, a per-record summary table is printed and each result
/// is appended as a JSON line to `target/benchmark-results.jsonl` (configurable
/// via `--results-log`). The JSONL file accumulates across runs so successive
/// results can be diffed, plotted, or fed into regression checks.
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 1, jvmArgs = {"-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints"})
@Tag("performance")
@CommandLine.Command(
    name = "predicate-bulk-bench",
    description = "JMH benchmarks for bulk predicate data access",
    mixinStandardHelpOptions = true
)
public class PredicateDataBulkAccessBenchmark implements Callable<Integer> {

    private static final long DEFAULT_SEED = 42L;
    private static final int DEFAULT_RECORDS = 100_000;
    private static final int DEFAULT_QUERIES = 1_000;
    private static final int DEFAULT_DIMENSION = 256;

    // -- Picocli data options --

    @CommandLine.Option(names = "--records", description = "Number of metadata records to generate (default: ${DEFAULT-VALUE})")
    private int records = DEFAULT_RECORDS;

    @CommandLine.Option(names = "--queries", description = "Number of predicate queries to generate (default: ${DEFAULT-VALUE})")
    private int queries = DEFAULT_QUERIES;

    @CommandLine.Option(names = "--dimension", description = "Vector dimension for generated data (default: ${DEFAULT-VALUE})")
    private int cliDimension = DEFAULT_DIMENSION;

    @CommandLine.Option(names = "--batch-size", arity = "1..*", split = ",",
        description = "Batch sizes as JMH @Param values (default: 100,1000,10000)")
    private int[] batchSizes = {100, 1000, 10000};

    @CommandLine.Option(names = "--backend", arity = "1..*", split = ",",
        description = "Backend types to benchmark as JMH @Param values (default: slab,sqlite)")
    private String[] backends = {"slab", "sqlite"};

    @CommandLine.Option(names = "--seed", description = "RNG seed for reproducible data gen (default: ${DEFAULT-VALUE})")
    private long seed = DEFAULT_SEED;

    // -- Picocli JMH tuning options --

    @CommandLine.Option(names = "--forks", description = "JMH fork count (default: ${DEFAULT-VALUE})")
    private int forks = 1;

    @CommandLine.Option(names = "--warmup", description = "Warmup iterations (default: ${DEFAULT-VALUE})")
    private int warmup = 2;

    @CommandLine.Option(names = "--iterations", description = "Measurement iterations (default: ${DEFAULT-VALUE})")
    private int iterations = 3;

    @CommandLine.Option(names = "--threads", description = "JMH threads (default: ${DEFAULT-VALUE})")
    private int threads = 1;

    @CommandLine.Option(names = "--output", description = "JMH JSON results path (default: ${DEFAULT-VALUE})")
    private String output = "target/predicate-bench-jmh-results.json";

    @CommandLine.Option(names = "--results-log", description = "Rolling JSONL results log (default: ${DEFAULT-VALUE})")
    private String resultsLog = "target/benchmark-results.jsonl";

    // -- JMH @Param fields (populated by JMH from OptionsBuilder.param()) --

    @Param({"100", "1000", "10000"})
    private int batchSize;

    @Param({"slab", "sqlite"})
    private String backendType;

    // -- Benchmark state --

    private TestDataGroup group;
    private PredicateTestDataView<?> predicateView;
    private Predicates<PNode<?>> predicatesDataset;
    private ResultIndices resultIndices;
    private MetadataContent metadataContent;
    private Random rng;
    private int predicateCount;
    private int contentCount;
    private int resultCount;

    /// Sets up the benchmark by generating (or reusing) the test dataset and
    /// opening the predicate views for the configured backend type. Data parameters
    /// are read from system properties set by the picocli {@link #call()} entry point,
    /// falling back to defaults for JUnit-based runs.
    ///
    /// @throws IOException if dataset loading fails
    @SuppressWarnings("unchecked")
    @Setup(Level.Trial)
    public void setup() throws IOException {
        int setupRecords = Integer.getInteger("bench.records", DEFAULT_RECORDS);
        int setupQueries = Integer.getInteger("bench.queries", DEFAULT_QUERIES);
        long setupSeed = Long.getLong("bench.seed", DEFAULT_SEED);
        int setupDimension = Integer.getInteger("bench.dimension", DEFAULT_DIMENSION);

        Path slabDir = Path.of("target/benchmark-data/predicate-slab");
        Path sqliteDir = Path.of("target/benchmark-data/predicate-sqlite");

        BenchmarkDataGenerator.ensureSlabDataset(slabDir, setupRecords, setupQueries, setupDimension, setupSeed);
        BenchmarkDataGenerator.ensureSqliteDataset(sqliteDir, slabDir, setupRecords, setupQueries, setupDimension, setupSeed);

        Path dataDir = "sqlite".equals(backendType) ? sqliteDir : slabDir;
        group = VectorTestData.load(dataDir);

        Optional<PredicateTestDataView<?>> predOpt = group.predicateProfile("default");
        predicateView = predOpt
            .orElseThrow(() -> new IllegalStateException("No predicate profile 'default' in dataset at " + dataDir));

        predicatesDataset = (Predicates<PNode<?>>) predicateView.getPredicatesView()
            .orElseThrow(() -> new IllegalStateException("No predicates view in dataset"));

        resultIndices = predicateView.getResultIndices().orElse(null);
        metadataContent = predicateView.getMetadataContentView().orElse(null);

        predicateCount = predicatesDataset.getCount();
        contentCount = metadataContent != null ? metadataContent.getCount() : 0;
        resultCount = resultIndices != null ? resultIndices.getCount() : 0;

        rng = new Random(setupSeed);
    }

    /// Closes the test data group and releases resources.
    ///
    /// @throws Exception if cleanup fails
    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        if (group != null) {
            group.close();
        }
    }

    /// Measures single random-access predicate read + PNode decode latency.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void singlePredicateGet(Blackhole bh) {
        long index = rng.nextInt(predicateCount);
        PNode<?> pred = predicatesDataset.get(index);
        bh.consume(pred);
    }

    /// Measures sequential predicate scan throughput.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void sequentialPredicateScan(Blackhole bh) {
        int count = 0;
        for (PNode<?> pred : predicatesDataset) {
            bh.consume(pred);
            count++;
            if (count >= batchSize) break;
        }
    }

    /// Measures result indices random-access latency.
    /// Skipped if result indices are not available.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void resultIndicesGet(Blackhole bh) {
        if (resultIndices == null || resultCount == 0) {
            return;
        }
        long index = rng.nextInt(resultCount);
        int[] indices = resultIndices.get(index);
        bh.consume(indices);
    }

    /// Measures result indices range read throughput.
    /// Skipped if result indices are not available.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void resultIndicesRange(Blackhole bh) {
        if (resultIndices == null || resultCount == 0) {
            return;
        }
        int effectiveBatch = Math.min(batchSize, resultCount);
        int start = rng.nextInt(Math.max(1, resultCount - effectiveBatch));
        int[][] range = resultIndices.getRange(start, start + effectiveBatch);
        bh.consume(range);
    }

    /// Measures metadata content random-access + Map decode latency.
    /// Skipped if metadata content is not available.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void metadataContentGet(Blackhole bh) {
        if (metadataContent == null || contentCount == 0) {
            return;
        }
        long index = rng.nextInt(contentCount);
        Map<String, Object> record = metadataContent.get(index);
        bh.consume(record);
    }

    /// Measures sequential metadata content scan throughput.
    /// Skipped if metadata content is not available.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void metadataContentScan(Blackhole bh) {
        if (metadataContent == null || contentCount == 0) {
            return;
        }
        int count = 0;
        for (Map<String, Object> record : metadataContent) {
            bh.consume(record);
            count++;
            if (count >= batchSize) break;
        }
    }

    /// Measures metadata layout cold load latency.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void metadataLayoutLoad(Blackhole bh) {
        Optional<MetadataLayout> layout = predicateView.getMetadataLayout();
        bh.consume(layout);
    }

    /// Measures predicate context resolution latency.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void predicateContextResolve(Blackhole bh) {
        Optional<PredicateContext> ctx = predicateView.getPredicateContext();
        bh.consume(ctx);
    }

    /// Picocli entry point. Sets system properties for data parameters, then
    /// builds and runs JMH with the configured options and JFR profiling.
    /// After the run completes, prints a per-record summary and appends
    /// results to the rolling JSONL log.
    ///
    /// @return exit code (0 for success)
    /// @throws Exception if the benchmark or results logging fails
    @Override
    public Integer call() throws Exception {
        System.setProperty("bench.records", String.valueOf(records));
        System.setProperty("bench.queries", String.valueOf(queries));
        System.setProperty("bench.seed", String.valueOf(seed));
        System.setProperty("bench.dimension", String.valueOf(cliDimension));

        ChainedOptionsBuilder ob = new OptionsBuilder()
            .include(getClass().getSimpleName())
            .forks(forks)
            .warmupIterations(warmup)
            .measurementIterations(iterations)
            .threads(threads)
            .addProfiler("jfr")
            .jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints",
                "-Dbench.records=" + records,
                "-Dbench.queries=" + queries,
                "-Dbench.seed=" + seed,
                "-Dbench.dimension=" + cliDimension)
            .resultFormat(ResultFormatType.JSON)
            .result(output);

        for (int bs : batchSizes) {
            ob = ob.param("batchSize", String.valueOf(bs));
        }
        for (String backend : backends) {
            ob = ob.param("backendType", backend);
        }

        Collection<RunResult> runResults = new Runner(ob.build()).run();

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("records", records);
        config.put("queries", queries);
        config.put("dimension", cliDimension);
        config.put("backends", Arrays.toString(backends));
        config.put("seed", seed);
        config.put("forks", forks);
        config.put("warmup", warmup);
        config.put("iterations", iterations);

        BenchmarkResults.summarizeAndLog(
            runResults, config, "Predicate Bulk Access Benchmark", resultsLog);

        return 0;
    }

    /// JUnit entry point for running benchmarks from `mvn test -Pperformance`.
    /// Tagged as "performance" so it is excluded from normal test runs.
    ///
    /// @throws RunnerException if the benchmark fails
    @Test
    @Tag("performance")
    void runBenchmarks() throws RunnerException {
        Options opts = new OptionsBuilder()
            .include(PredicateDataBulkAccessBenchmark.class.getSimpleName())
            .param("backendType", "slab")
            .param("batchSize", "1000")
            .warmupIterations(1)
            .measurementIterations(2)
            .forks(1)
            .addProfiler("jfr")
            .jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints")
            .resultFormat(ResultFormatType.JSON)
            .result("target/predicate-bench-jmh-results.json")
            .build();
        new Runner(opts).run();
    }

    /// Standalone entry point. Parses picocli options and delegates to {@link #call()}.
    ///
    /// @param args command-line arguments
    public static void main(String[] args) {
        int exitCode = new CommandLine(new PredicateDataBulkAccessBenchmark()).execute(args);
        System.exit(exitCode);
    }
}
