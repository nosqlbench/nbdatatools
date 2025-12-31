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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance tests for VectorGen.
 * These tests measure throughput and verify O(1) sampling behavior.
 * Run with the performance-tests profile or -DskipPerformanceTests=false.
 */
class VectorGenPerfTest {

    private static final int WARMUP_ITERATIONS = 10_000;
    private static final int BENCHMARK_ITERATIONS = 100_000;

    @Test
    void testSingleVectorThroughput() {
        VectorSpaceModel model = new VectorSpaceModel(10_000_000, 128, 0.0, 1.0);
        VectorGen gen = new VectorGen(model);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            gen.apply(i);
        }

        // Benchmark
        long start = System.nanoTime();
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            gen.apply(i);
        }
        long elapsed = System.nanoTime() - start;

        double opsPerSecond = BENCHMARK_ITERATIONS / (elapsed / 1_000_000_000.0);
        double nsPerOp = (double) elapsed / BENCHMARK_ITERATIONS;

        System.out.printf("Single vector generation (128-dim):%n");
        System.out.printf("  %.2f ops/sec%n", opsPerSecond);
        System.out.printf("  %.2f ns/op%n", nsPerOp);

        // Verify reasonable performance (at least 100K ops/sec for 128-dim)
        assertTrue(opsPerSecond > 100_000,
            "Expected > 100K ops/sec, got: " + opsPerSecond);
    }

    @Test
    void testBatchThroughput() {
        VectorSpaceModel model = new VectorSpaceModel(10_000_000, 128, 0.0, 1.0);
        VectorGen gen = new VectorGen(model);

        int batchSize = 1000;
        int batches = BENCHMARK_ITERATIONS / batchSize;

        // Warmup
        for (int i = 0; i < 100; i++) {
            gen.generateFlatBatch(i * batchSize, batchSize);
        }

        // Benchmark
        long start = System.nanoTime();
        for (int i = 0; i < batches; i++) {
            gen.generateFlatBatch(i * batchSize, batchSize);
        }
        long elapsed = System.nanoTime() - start;

        long totalVectors = (long) batches * batchSize;
        double vectorsPerSecond = totalVectors / (elapsed / 1_000_000_000.0);
        double nsPerVector = (double) elapsed / totalVectors;

        System.out.printf("Batch generation (128-dim, batch=%d):%n", batchSize);
        System.out.printf("  %.2f vectors/sec%n", vectorsPerSecond);
        System.out.printf("  %.2f ns/vector%n", nsPerVector);

        // Batch should be faster per-vector than single generation
        assertTrue(vectorsPerSecond > 100_000,
            "Expected > 100K vectors/sec, got: " + vectorsPerSecond);
    }

    @Test
    void testO1SamplingCost() {
        // Verify that sampling cost doesn't depend on ordinal value
        VectorSpaceModel model = new VectorSpaceModel(Long.MAX_VALUE / 2, 64, 0.0, 1.0);
        VectorGen gen = new VectorGen(model);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            gen.apply(i);
        }

        // Time small ordinals
        long start1 = System.nanoTime();
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            gen.apply(i);
        }
        long elapsed1 = System.nanoTime() - start1;

        // Time large ordinals
        long largeBase = Long.MAX_VALUE / 4;
        long start2 = System.nanoTime();
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            gen.apply(largeBase + i);
        }
        long elapsed2 = System.nanoTime() - start2;

        double ratio = (double) elapsed2 / elapsed1;

        System.out.printf("O(1) verification:%n");
        System.out.printf("  Small ordinals: %.2f ns/op%n", (double) elapsed1 / BENCHMARK_ITERATIONS);
        System.out.printf("  Large ordinals: %.2f ns/op%n", (double) elapsed2 / BENCHMARK_ITERATIONS);
        System.out.printf("  Ratio: %.2f%n", ratio);

        // Times should be within 50% of each other (O(1) behavior)
        assertTrue(ratio > 0.5 && ratio < 1.5,
            "Expected O(1) behavior, but ratio was: " + ratio);
    }

    @Test
    void testDimensionScaling() {
        // Verify linear scaling with dimensions
        int[] dimensions = {32, 64, 128, 256, 512};
        double[] nsPerDim = new double[dimensions.length];

        for (int d = 0; d < dimensions.length; d++) {
            int dim = dimensions[d];
            VectorSpaceModel model = new VectorSpaceModel(1_000_000, dim, 0.0, 1.0);
            VectorGen gen = new VectorGen(model);

            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                gen.apply(i);
            }

            // Benchmark
            long start = System.nanoTime();
            for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
                gen.apply(i);
            }
            long elapsed = System.nanoTime() - start;

            double nsPerOp = (double) elapsed / BENCHMARK_ITERATIONS;
            nsPerDim[d] = nsPerOp / dim;

            System.out.printf("Dimension %d: %.2f ns/op (%.4f ns/dim)%n", dim, nsPerOp, nsPerDim[d]);
        }

        // ns/dim should be relatively constant across dimensions (linear scaling)
        double avgNsPerDim = 0;
        for (double ns : nsPerDim) avgNsPerDim += ns;
        avgNsPerDim /= nsPerDim.length;

        for (int d = 0; d < dimensions.length; d++) {
            double deviation = Math.abs(nsPerDim[d] - avgNsPerDim) / avgNsPerDim;
            assertTrue(deviation < 0.5,
                "Expected linear scaling, but dimension " + dimensions[d] +
                " deviated by " + (deviation * 100) + "%");
        }
    }
}
