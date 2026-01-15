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
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for DimensionStatistics performance comparison.
 *
 * <h2>Purpose</h2>
 *
 * <p>Measures the throughput and latency of statistics computation
 * across various data sizes to characterize performance and identify
 * optimization opportunities.
 *
 * <h2>Benchmark Configurations</h2>
 *
 * <ul>
 *   <li>Data sizes: 1K, 8K, 64K, 512K elements</li>
 *   <li>Both float[] and double[] input types</li>
 *   <li>Throughput measured in ops/second</li>
 * </ul>
 *
 * <h2>Running</h2>
 *
 * <pre>{@code
 * mvn test -pl datatools-vshapes -Pperformance -Dtest=DimensionStatisticsJmhBenchmark
 * }</pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgs = {"--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED"})
@Tag("performance")
public class DimensionStatisticsJmhBenchmark {

    @Param({"1024", "8192", "65536", "524288"})
    private int dataSize;

    private float[] floatData;
    private double[] doubleData;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(42);  // Deterministic for reproducibility

        floatData = new float[dataSize];
        doubleData = new double[dataSize];

        // Generate normal-like data
        for (int i = 0; i < dataSize; i++) {
            double value = random.nextGaussian() * 0.5 + 0.5;
            floatData[i] = (float) value;
            doubleData[i] = value;
        }
    }

    @Benchmark
    public void computeFromFloat(Blackhole bh) {
        DimensionStatistics stats = DimensionStatistics.compute(0, floatData);
        bh.consume(stats);
    }

    @Benchmark
    public void computeFromDouble(Blackhole bh) {
        DimensionStatistics stats = DimensionStatistics.compute(0, doubleData);
        bh.consume(stats);
    }

    /**
     * Runs the JMH benchmark from JUnit.
     * Enable with: mvn test -pl datatools-vshapes -Pperformance
     */
    @Test
    @Tag("performance")
    public void runBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(DimensionStatisticsJmhBenchmark.class.getSimpleName())
            .warmupIterations(2)
            .measurementIterations(3)
            .forks(1)
            .jvmArgs("--add-modules", "jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED")
            .resultFormat(ResultFormatType.TEXT)
            .build();

        new Runner(opt).run();
    }
}
