package io.nosqlbench.vshapes.extract;

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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark comparing sequential vs batched dimension statistics.
 *
 * <h2>Purpose</h2>
 *
 * <p>Measures the speedup achieved by processing 8 dimensions in parallel
 * using AVX-512 SIMD operations versus sequential per-dimension processing.
 *
 * <h2>Test Configurations</h2>
 *
 * <ul>
 *   <li>Dimensions: 64, 256, 1024, 4096</li>
 *   <li>Vectors: 10K, 100K</li>
 *   <li>Compares: Sequential vs Batched (8-wide SIMD)</li>
 * </ul>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 2)
@Fork(value = 1, jvmArgs = {"--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@Tag("performance")
public class BatchStatisticsBenchmark {

    @Param({"64", "256", "1024"})
    private int dimensions;

    @Param({"10000", "100000"})
    private int vectors;

    private float[][] data;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(42);
        data = new float[vectors][dimensions];

        for (int v = 0; v < vectors; v++) {
            for (int d = 0; d < dimensions; d++) {
                // Different distribution per dimension for realism
                double mean = 0.5 + d * 0.001;
                double stdDev = 0.1 + (d % 10) * 0.01;
                data[v][d] = (float) (random.nextGaussian() * stdDev + mean);
            }
        }
    }

    /**
     * Baseline: Sequential per-dimension statistics computation.
     */
    @Benchmark
    public void sequentialStatistics(Blackhole bh) {
        for (int d = 0; d < dimensions; d++) {
            // Extract dimension data
            float[] dimData = new float[vectors];
            for (int v = 0; v < vectors; v++) {
                dimData[v] = data[v][d];
            }

            DimensionStatistics stats = DimensionStatistics.compute(d, dimData);
            bh.consume(stats);
        }
    }

    /**
     * Optimized: Batched 8-dimension SIMD statistics computation.
     */
    @Benchmark
    public void batchedStatistics(Blackhole bh) {
        DimensionStatistics[] allStats = BatchDimensionStatistics.computeAll(data);
        bh.consume(allStats);
    }

    /**
     * Runs the benchmark from JUnit.
     */
    @Test
    @Tag("performance")
    public void runBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(BatchStatisticsBenchmark.class.getSimpleName())
            .warmupIterations(2)
            .measurementIterations(3)
            .forks(1)
            .jvmArgs("--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED")
            .build();

        new Runner(opt).run();
    }
}
