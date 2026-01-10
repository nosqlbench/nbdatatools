package io.nosqlbench.vectordata.views;

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

import io.nosqlbench.datatools.virtdata.DimensionDistributionGenerator;
import io.nosqlbench.datatools.virtdata.VectorGenerator;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/// JMH benchmarks for VirtdataFloatVectorsView performance.
///
/// ## Benchmark Categories
///
/// - **Single Vector Access**: Measures latency of `get(index)` calls
/// - **Batch Range Access**: Measures throughput of `getRange(start, end)` calls
/// - **Indexed Access**: Measures performance of `getIndexed` variants
///
/// ## Running the Benchmarks
///
/// ```bash
/// mvn test-compile exec:java \
///     -Dexec.mainClass="io.nosqlbench.vectordata.views.VirtdataFloatVectorsViewJmhBenchmark" \
///     -Dexec.classpathScope=test -pl datatools-vectordata
/// ```
///
/// Or run the main method directly from an IDE.
///
/// @see VirtdataFloatVectorsView
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgs = {"--add-modules", "jdk.incubator.vector"})
@Tag("performance")
public class VirtdataFloatVectorsViewJmhBenchmark {

    @Param({"64", "128", "256", "512"})
    private int dimensions;

    @Param({"100", "1000", "10000"})
    private int batchSize;

    private VirtdataFloatVectorsView boundedView;
    private VirtdataFloatVectorsView unboundedView;
    private long ordinal;

    @Setup(Level.Trial)
    public void setup() {
        VectorSpaceModel model = new VectorSpaceModel(10_000_000L, dimensions, 0.0, 1.0);
        VectorGenerator<VectorSpaceModel> generator = new DimensionDistributionGenerator(model);

        boundedView = new VirtdataFloatVectorsView(generator, 1_000_000);
        unboundedView = new VirtdataFloatVectorsView(generator);
        ordinal = 0;
    }

    // ==================== Single Vector Access ====================

    @Benchmark
    public void singleVector_Bounded(Blackhole bh) {
        bh.consume(boundedView.get(ordinal++));
    }

    @Benchmark
    public void singleVector_Unbounded(Blackhole bh) {
        bh.consume(unboundedView.get(ordinal++));
    }

    // ==================== Batch Range Access ====================

    @Benchmark
    public void batchRange_Bounded(Blackhole bh) {
        bh.consume(boundedView.getRange(ordinal, ordinal + batchSize));
        ordinal += batchSize;
    }

    @Benchmark
    public void batchRange_Unbounded(Blackhole bh) {
        bh.consume(unboundedView.getRange(ordinal, ordinal + batchSize));
        ordinal += batchSize;
    }

    // ==================== Indexed Access ====================

    @Benchmark
    public void indexedSingle_Bounded(Blackhole bh) {
        bh.consume(boundedView.getIndexed(ordinal++));
    }

    @Benchmark
    public void indexedRange_Bounded(Blackhole bh) {
        bh.consume(boundedView.getIndexedRange(ordinal, ordinal + batchSize));
        ordinal += batchSize;
    }

    // ==================== Async Access (should complete immediately) ====================

    @Benchmark
    public void asyncSingle_Bounded(Blackhole bh) throws Exception {
        bh.consume(boundedView.getAsync(ordinal++).get());
    }

    @Benchmark
    public void asyncRange_Bounded(Blackhole bh) throws Exception {
        bh.consume(boundedView.getRangeAsync(ordinal, ordinal + batchSize).get());
        ordinal += batchSize;
    }

    /// Runs the JMH benchmark from JUnit.
    ///
    /// Enable with: `mvn test -pl datatools-vectordata -Pperformance`
    @Test
    @Tag("performance")
    public void runBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(VirtdataFloatVectorsViewJmhBenchmark.class.getSimpleName())
            .warmupIterations(2)
            .measurementIterations(3)
            .forks(1)
            .jvmArgs("--add-modules", "jdk.incubator.vector")
            .resultFormat(ResultFormatType.TEXT)
            .build();

        new Runner(opt).run();
    }

    /// Entry point for running benchmarks from command line.
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(VirtdataFloatVectorsViewJmhBenchmark.class.getSimpleName())
            .forks(1)
            .jvmArgs("--add-modules", "jdk.incubator.vector")
            .build();

        new Runner(opt).run();
    }
}
