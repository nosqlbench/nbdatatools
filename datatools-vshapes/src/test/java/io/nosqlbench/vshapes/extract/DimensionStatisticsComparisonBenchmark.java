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
 * JMH benchmark comparing scalar vs Panama Vector API implementations.
 *
 * <h2>Purpose</h2>
 *
 * <p>Directly compares the performance of the scalar (Java 11) implementation
 * against the Panama Vector API (Java 25) implementation to measure SIMD speedup.
 *
 * <h2>Running</h2>
 *
 * <pre>{@code
 * mvn test -pl datatools-vshapes -Dtest=DimensionStatisticsComparisonBenchmark
 * }</pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgs = {"--add-modules", "jdk.incubator.vector"})
@Tag("performance")
public class DimensionStatisticsComparisonBenchmark {

    @Param({"1024", "8192", "65536", "524288"})
    private int dataSize;

    private double[] doubleData;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(42);
        doubleData = new double[dataSize];
        for (int i = 0; i < dataSize; i++) {
            doubleData[i] = random.nextGaussian() * 0.5 + 0.5;
        }
    }

    /**
     * Benchmark the current implementation (Panama on JDK 25).
     */
    @Benchmark
    public void panamaImplementation(Blackhole bh) {
        DimensionStatistics stats = DimensionStatistics.compute(0, doubleData);
        bh.consume(stats);
    }

    /**
     * Benchmark the scalar implementation (baseline).
     */
    @Benchmark
    public void scalarImplementation(Blackhole bh) {
        DimensionStatistics stats = computeScalar(0, doubleData);
        bh.consume(stats);
    }

    /**
     * Scalar implementation (copy of Java 11 version for comparison).
     */
    private static DimensionStatistics computeScalar(int dimension, double[] values) {
        long count = values.length;

        // First pass: min, max, mean
        double min = values[0];
        double max = values[0];
        double sum = 0;

        for (double v : values) {
            if (v < min) min = v;
            if (v > max) max = v;
            sum += v;
        }

        double mean = sum / count;

        // Second pass: variance, skewness, kurtosis
        double m2 = 0;
        double m3 = 0;
        double m4 = 0;

        for (double v : values) {
            double diff = v - mean;
            double diff2 = diff * diff;
            m2 += diff2;
            m3 += diff2 * diff;
            m4 += diff2 * diff2;
        }

        double variance = m2 / count;
        double stdDev = Math.sqrt(variance);

        double skewness = 0;
        double kurtosis = 3;

        if (stdDev > 0) {
            skewness = (m3 / count) / (stdDev * stdDev * stdDev);
            kurtosis = (m4 / count) / (variance * variance);
        }

        return new DimensionStatistics(dimension, count, min, max, mean, variance, skewness, kurtosis);
    }

    @Test
    @Tag("performance")
    public void runComparisonBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(DimensionStatisticsComparisonBenchmark.class.getSimpleName())
            .warmupIterations(2)
            .measurementIterations(3)
            .forks(1)
            .jvmArgs("--add-modules", "jdk.incubator.vector")
            .build();

        new Runner(opt).run();
    }
}
