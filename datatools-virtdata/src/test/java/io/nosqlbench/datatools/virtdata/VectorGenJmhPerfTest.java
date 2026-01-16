package io.nosqlbench.datatools.virtdata;

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

import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JMH benchmarks comparing Panama SIMD vs Scalar implementations of VectorGen.
 * Tests both single-threaded and multi-threaded performance.
 *
 * <p>Run with: {@code mvn test -pl datatools-virtdata -Dtest=VectorGenJmhPerfTest}
 *
 * <p>This test is tagged as "performance" to allow exclusion from quick CI runs.
 * Use {@code -DexcludedGroups=performance} to skip.
 */
@Tag("performance")
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1, jvmArgs = {"--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
public class VectorGenJmhPerfTest {

    @Param({"128"})
    private int dimensions;

    @Param({"100"})
    private int batchSize;

    private ScalarDimensionDistributionGenerator scalarGen;
    private DimensionDistributionGenerator panamaGen;

    // Thread-safe ordinal counter for multi-threaded tests
    private AtomicLong ordinalCounter;

    @Setup(Level.Trial)
    public void setup() {
        VectorSpaceModel model = new VectorSpaceModel(10_000_000L, dimensions, 0.0, 1.0);
        scalarGen = new ScalarDimensionDistributionGenerator(model);
        panamaGen = new DimensionDistributionGenerator(model);
        ordinalCounter = new AtomicLong(0);

        System.out.println("Panama available: " + VectorGenFactory.isPanamaAvailable());
        System.out.println("Available processors: " + Runtime.getRuntime().availableProcessors());
    }

    @Benchmark
    @Threads(1)
    public void singleVector_Panama_1T(Blackhole bh) {
        bh.consume(panamaGen.apply(ordinalCounter.getAndIncrement()));
    }

    @Benchmark
    @Threads(1)
    public void singleVector_Scalar_1T(Blackhole bh) {
        bh.consume(scalarGen.apply(ordinalCounter.getAndIncrement()));
    }

    @Benchmark
    @Threads(2)
    public void singleVector_Panama_2T(Blackhole bh) {
        bh.consume(panamaGen.apply(ordinalCounter.getAndIncrement()));
    }

    @Benchmark
    @Threads(2)
    public void singleVector_Scalar_2T(Blackhole bh) {
        bh.consume(scalarGen.apply(ordinalCounter.getAndIncrement()));
    }

    @Benchmark
    @Threads(4)
    public void singleVector_Panama_4T(Blackhole bh) {
        bh.consume(panamaGen.apply(ordinalCounter.getAndIncrement()));
    }

    @Benchmark
    @Threads(4)
    public void singleVector_Scalar_4T(Blackhole bh) {
        bh.consume(scalarGen.apply(ordinalCounter.getAndIncrement()));
    }

    @Benchmark
    @Threads(8)
    public void singleVector_Panama_8T(Blackhole bh) {
        bh.consume(panamaGen.apply(ordinalCounter.getAndIncrement()));
    }

    @Benchmark
    @Threads(8)
    public void singleVector_Scalar_8T(Blackhole bh) {
        bh.consume(scalarGen.apply(ordinalCounter.getAndIncrement()));
    }

    @Benchmark
    @Threads(1)
    public void batchFlat_Panama_1T(Blackhole bh) {
        long start = ordinalCounter.getAndAdd(batchSize);
        bh.consume(panamaGen.generateFlatBatch(start, batchSize));
    }

    @Benchmark
    @Threads(1)
    public void batchFlat_Scalar_1T(Blackhole bh) {
        long start = ordinalCounter.getAndAdd(batchSize);
        bh.consume(scalarGen.generateFlatBatch(start, batchSize));
    }

    @Benchmark
    @Threads(4)
    public void batchFlat_Panama_4T(Blackhole bh) {
        long start = ordinalCounter.getAndAdd(batchSize);
        bh.consume(panamaGen.generateFlatBatch(start, batchSize));
    }

    @Benchmark
    @Threads(4)
    public void batchFlat_Scalar_4T(Blackhole bh) {
        long start = ordinalCounter.getAndAdd(batchSize);
        bh.consume(scalarGen.generateFlatBatch(start, batchSize));
    }

    @Benchmark
    @Threads(8)
    public void batchFlat_Panama_8T(Blackhole bh) {
        long start = ordinalCounter.getAndAdd(batchSize);
        bh.consume(panamaGen.generateFlatBatch(start, batchSize));
    }

    @Benchmark
    @Threads(8)
    public void batchFlat_Scalar_8T(Blackhole bh) {
        long start = ordinalCounter.getAndAdd(batchSize);
        bh.consume(scalarGen.generateFlatBatch(start, batchSize));
    }

    /**
     * JUnit test that runs the JMH benchmarks.
     * Uses reduced iterations for faster CI while still providing useful performance data.
     */
    @Test
    void runJmhBenchmarks() throws RunnerException {
        System.out.println("\n========== VectorGen JMH Benchmark: Panama vs Scalar ==========");
        System.out.println("Panama available: " + VectorGenFactory.isPanamaAvailable());
        System.out.println("Available processors: " + Runtime.getRuntime().availableProcessors());
        System.out.println("================================================================\n");

        Options opt = new OptionsBuilder()
            .include(VectorGenJmhPerfTest.class.getSimpleName())
            .warmupIterations(3)
            .warmupTime(org.openjdk.jmh.runner.options.TimeValue.seconds(1))
            .measurementIterations(3)
            .measurementTime(org.openjdk.jmh.runner.options.TimeValue.seconds(1))
            .forks(1)
            .jvmArgs("--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED")
            .build();

        new Runner(opt).run();
    }
}
