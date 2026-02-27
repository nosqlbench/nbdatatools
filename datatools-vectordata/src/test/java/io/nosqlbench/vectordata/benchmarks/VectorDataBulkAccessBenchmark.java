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
import io.nosqlbench.vectordata.discovery.vector.VectorTestDataView;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborDistances;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborIndices;
import io.nosqlbench.vectordata.spec.datasets.types.QueryVectors;
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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/// JMH benchmarks for bulk vector data access through {@link VectorTestData}.
///
/// Exercises realistic I/O paths against generated predicated datasets to measure
/// the throughput and latency of vector reads, range scans, and neighbor index
/// lookups. Data is generated once via {@link BenchmarkDataGenerator} and cached in
/// `target/benchmark-data/`.
///
/// ## Running via exec:exec
///
/// ```bash
/// # Show all options
/// mvn exec:exec@bench-vector-bulk -pl datatools-vectordata -Dbench.args="--help"
///
/// # Quick smoke test
/// mvn exec:exec@bench-vector-bulk -pl datatools-vectordata \
///   -Dbench.args="--records 10000 --dimension 128 --batch-size 100 --warmup 1 --iterations 1"
///
/// # Full run with 768d vectors
/// mvn exec:exec@bench-vector-bulk -pl datatools-vectordata \
///   -Dbench.args="--records 1000000 --dimension 768"
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
    name = "vector-bulk-bench",
    description = "JMH benchmarks for bulk vector data access",
    mixinStandardHelpOptions = true
)
public class VectorDataBulkAccessBenchmark implements Callable<Integer> {

    private static final long DEFAULT_SEED = 42L;
    private static final int DEFAULT_RECORDS = 100_000;
    private static final int DEFAULT_QUERIES = 1_000;

    // -- Picocli data options --

    @CommandLine.Option(names = "--records", description = "Number of base vectors to generate (default: ${DEFAULT-VALUE})")
    private int records = DEFAULT_RECORDS;

    @CommandLine.Option(names = "--queries", description = "Number of query vectors to generate (default: ${DEFAULT-VALUE})")
    private int queries = DEFAULT_QUERIES;

    @CommandLine.Option(names = "--dimension", arity = "1..*", split = ",",
        description = "Vector dimensions as JMH @Param values (default: 256)")
    private int[] dimensions = {256};

    @CommandLine.Option(names = "--batch-size", arity = "1..*", split = ",",
        description = "Batch sizes as JMH @Param values (default: 100,1000,10000)")
    private int[] batchSizes = {100, 1000, 10000};

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
    private String output = "target/vector-bench-jmh-results.json";

    @CommandLine.Option(names = "--results-log", description = "Rolling JSONL results log (default: ${DEFAULT-VALUE})")
    private String resultsLog = "target/benchmark-results.jsonl";

    // -- JMH @Param fields (populated by JMH from OptionsBuilder.param()) --

    @Param({"100", "1000", "10000"})
    private int batchSize;

    @Param({"256"})
    private int dimension;

    // -- Benchmark state --

    private TestDataGroup group;
    private VectorTestDataView view;
    private BaseVectors baseVectors;
    private QueryVectors queryVectors;
    private NeighborIndices neighborIndices;
    private NeighborDistances neighborDistances;
    private Random rng;
    private int baseCount;
    private int queryCount;

    /// Sets up the benchmark by generating (or reusing) the test dataset and
    /// opening all vector views. Data parameters are read from system properties
    /// set by the picocli {@link #call()} entry point, falling back to defaults
    /// for JUnit-based runs.
    ///
    /// @throws IOException if dataset loading fails
    @Setup(Level.Trial)
    public void setup() throws IOException {
        int setupRecords = Integer.getInteger("bench.records", DEFAULT_RECORDS);
        int setupQueries = Integer.getInteger("bench.queries", DEFAULT_QUERIES);
        long setupSeed = Long.getLong("bench.seed", DEFAULT_SEED);

        Path dataDir = Path.of("target/benchmark-data/vector-bench-" + dimension + "d");
        BenchmarkDataGenerator.ensureSlabDataset(dataDir, setupRecords, setupQueries, dimension, setupSeed);

        group = VectorTestData.load(dataDir);
        view = group.getDefaultProfile();

        baseVectors = view.getBaseVectors()
            .orElseThrow(() -> new IllegalStateException("No base vectors in dataset"));
        queryVectors = view.getQueryVectors()
            .orElseThrow(() -> new IllegalStateException("No query vectors in dataset"));

        baseCount = baseVectors.getCount();
        queryCount = queryVectors.getCount();

        neighborIndices = view.getNeighborIndices().orElse(null);
        neighborDistances = view.getNeighborDistances().orElse(null);

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

    /// Measures single random-access vector read latency.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void singleVectorGet(Blackhole bh) {
        long index = rng.nextInt(baseCount);
        float[] vec = baseVectors.get(index);
        bh.consume(vec);
    }

    /// Measures sequential range read throughput over base vectors.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void sequentialRange(Blackhole bh) {
        int start = rng.nextInt(Math.max(1, baseCount - batchSize));
        float[][] range = baseVectors.getRange(start, start + Math.min(batchSize, baseCount - start));
        bh.consume(range);
    }

    /// Measures random-start range read throughput over base vectors.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void randomRange(Blackhole bh) {
        int start = rng.nextInt(Math.max(1, baseCount - batchSize));
        float[][] range = baseVectors.getRange(start, start + Math.min(batchSize, baseCount - start));
        bh.consume(range);
    }

    /// Measures query vector iteration throughput.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void queryVectorIteration(Blackhole bh) {
        int count = 0;
        for (float[] vec : queryVectors) {
            bh.consume(vec);
            count++;
            if (count >= batchSize) break;
        }
    }

    /// Measures neighbor indices random-access latency.
    /// Skipped if neighbor indices are not available.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void neighborIndicesGet(Blackhole bh) {
        if (neighborIndices == null) {
            return;
        }
        long index = rng.nextInt(neighborIndices.getCount());
        int[] indices = neighborIndices.get(index);
        bh.consume(indices);
    }

    /// Measures neighbor distances range read throughput.
    /// Skipped if neighbor distances are not available.
    ///
    /// @param bh the JMH blackhole to consume results
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void neighborDistancesRange(Blackhole bh) {
        if (neighborDistances == null) {
            return;
        }
        int count = neighborDistances.getCount();
        int start = rng.nextInt(Math.max(1, count - batchSize));
        int end = start + Math.min(batchSize, count - start);
        float[][] range = neighborDistances.getRange(start, end);
        bh.consume(range);
    }

    /// Measures prebuffer followed by sequential scan latency.
    ///
    /// @param bh the JMH blackhole to consume results
    /// @throws Exception if prebuffering fails
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void prebufferThenRead(Blackhole bh) throws Exception {
        baseVectors.prebuffer().get();
        for (int i = 0; i < Math.min(batchSize, baseCount); i++) {
            float[] vec = baseVectors.get(i);
            bh.consume(vec);
        }
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
                "-Dbench.seed=" + seed)
            .resultFormat(ResultFormatType.JSON)
            .result(output);

        for (int d : dimensions) {
            ob = ob.param("dimension", String.valueOf(d));
        }
        for (int bs : batchSizes) {
            ob = ob.param("batchSize", String.valueOf(bs));
        }

        Collection<RunResult> runResults = new Runner(ob.build()).run();

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("records", records);
        config.put("queries", queries);
        config.put("dimensions", Arrays.toString(dimensions));
        config.put("seed", seed);
        config.put("forks", forks);
        config.put("warmup", warmup);
        config.put("iterations", iterations);

        BenchmarkResults.summarizeAndLog(
            runResults, config, "Vector Bulk Access Benchmark", resultsLog);

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
            .include(VectorDataBulkAccessBenchmark.class.getSimpleName())
            .param("dimension", "256")
            .param("batchSize", "1000")
            .warmupIterations(1)
            .measurementIterations(2)
            .forks(1)
            .addProfiler("jfr")
            .jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints")
            .resultFormat(ResultFormatType.JSON)
            .result("target/vector-bench-jmh-results.json")
            .build();
        new Runner(opts).run();
    }

    /// Standalone entry point. Parses picocli options and delegates to {@link #call()}.
    ///
    /// @param args command-line arguments
    public static void main(String[] args) {
        int exitCode = new CommandLine(new VectorDataBulkAccessBenchmark()).execute(args);
        System.exit(exitCode);
    }
}
