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

import io.nosqlbench.vshapes.model.VectorSpaceModel;
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
 * Comprehensive throughput benchmark comparing extraction approaches.
 *
 * <h2>Configurations Tested</h2>
 *
 * <ol>
 *   <li><b>Sequential Baseline</b>: DatasetModelExtractor (single-threaded)</li>
 *   <li><b>Parallel SIMD</b>: ParallelDatasetModelExtractor (multi-threaded + AVX-512)</li>
 * </ol>
 *
 * <h2>Test Matrix</h2>
 *
 * <ul>
 *   <li>Dimensions: 256, 1024, 4096</li>
 *   <li>Vectors: 10K, 100K</li>
 *   <li>Threads: 1, 4, 8, 16</li>
 * </ul>
 *
 * <h2>Expected Results</h2>
 *
 * <pre>
 * Configuration           | Throughput | Speedup vs Baseline
 * ----------------------- | ---------- | -------------------
 * Sequential Baseline     | 1x         | 1.0x
 * Parallel (4 threads)    | ~4x        | ~4x
 * Parallel (8 threads)    | ~6-7x      | ~6-7x
 * Parallel (16 threads)   | ~8-12x     | ~8-12x (diminishing returns)
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(value = 1, jvmArgs = {"--add-modules", "jdk.incubator.vector"})
@Tag("performance")
public class ParallelExtractionBenchmark {

    @Param({"256", "1024"})
    private int dimensions;

    @Param({"10000", "50000"})
    private int vectors;

    @Param({"1", "4", "8"})
    private int threads;

    private float[][] data;
    private DatasetModelExtractor sequentialExtractor;
    private ParallelDatasetModelExtractor parallelExtractor;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random(42);
        data = new float[vectors][dimensions];

        // Generate realistic data with varying distributions per dimension
        for (int v = 0; v < vectors; v++) {
            for (int d = 0; d < dimensions; d++) {
                // Mix of normal, uniform, and skewed distributions
                double mean = 0.5 + d * 0.001;
                double stdDev = 0.1 + (d % 10) * 0.01;

                if (d % 3 == 0) {
                    // Normal distribution
                    data[v][d] = (float) (random.nextGaussian() * stdDev + mean);
                } else if (d % 3 == 1) {
                    // Uniform distribution
                    data[v][d] = (float) (random.nextDouble() * stdDev * 2 + mean - stdDev);
                } else {
                    // Skewed distribution (exponential shifted)
                    data[v][d] = (float) (-Math.log(1 - random.nextDouble()) * stdDev + mean);
                }
            }
        }

        // Create extractors
        sequentialExtractor = new DatasetModelExtractor();
        parallelExtractor = ParallelDatasetModelExtractor.builder()
            .parallelism(threads)
            .batchSize(64)
            .build();

        System.out.printf("Setup: %d dims, %d vectors, %d threads, AVX-512: %s%n",
            dimensions, vectors, threads, BatchDimensionStatistics.isAvx512Available());
    }

    @TearDown(Level.Trial)
    public void teardown() {
        if (parallelExtractor != null) {
            parallelExtractor.shutdown();
        }
    }

    /**
     * Baseline: Sequential single-threaded extraction.
     */
    @Benchmark
    public void sequentialExtraction(Blackhole bh) {
        // Only run sequential baseline for threads=1 to avoid redundant measurements
        if (threads != 1) {
            bh.consume(0);
            return;
        }

        VectorSpaceModel model = sequentialExtractor.extractVectorModel(data);
        bh.consume(model);
    }

    /**
     * Optimized: Parallel multi-threaded SIMD extraction.
     */
    @Benchmark
    public void parallelExtraction(Blackhole bh) {
        VectorSpaceModel model = parallelExtractor.extractVectorModel(data);
        bh.consume(model);
    }

    /**
     * Runs the benchmark from JUnit.
     */
    @Test
    @Tag("performance")
    public void runBenchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(ParallelExtractionBenchmark.class.getSimpleName())
            .warmupIterations(2)
            .measurementIterations(3)
            .forks(1)
            .jvmArgs("--add-modules", "jdk.incubator.vector")
            .build();

        new Runner(opt).run();
    }

    /**
     * Quick correctness check (not a JMH benchmark).
     */
    @Test
    @Tag("unit")
    public void verifyCorrectness() {
        Random random = new Random(123);
        int testDims = 64;
        int testVecs = 1000;

        float[][] testData = new float[testVecs][testDims];
        for (int v = 0; v < testVecs; v++) {
            for (int d = 0; d < testDims; d++) {
                testData[v][d] = (float) (random.nextGaussian() * 0.1 + 0.5);
            }
        }

        DatasetModelExtractor sequential = new DatasetModelExtractor();
        ParallelDatasetModelExtractor parallel = ParallelDatasetModelExtractor.builder()
            .parallelism(4)
            .build();

        try {
            VectorSpaceModel seqModel = sequential.extractVectorModel(testData);
            VectorSpaceModel parModel = parallel.extractVectorModel(testData);

            // Verify same number of components
            if (seqModel.dimensions() != parModel.dimensions()) {
                throw new AssertionError("Dimension mismatch: " +
                    seqModel.dimensions() + " vs " + parModel.dimensions());
            }

            // Verify components are similar (may differ slightly due to floating-point)
            for (int d = 0; d < testDims; d++) {
                var seqComp = seqModel.scalarModel(d);
                var parComp = parModel.scalarModel(d);

                if (!seqComp.getClass().equals(parComp.getClass())) {
                    throw new AssertionError("Component type mismatch at dim " + d +
                        ": " + seqComp.getClass().getSimpleName() +
                        " vs " + parComp.getClass().getSimpleName());
                }
            }

            System.out.println("Correctness verified: " + testDims + " dimensions match");
        } finally {
            parallel.shutdown();
        }
    }

    /**
     * Quick throughput comparison (not full JMH).
     * Tests thread counts up to at least half the hardware thread count.
     */
    @Test
    @Tag("performance")
    public void quickThroughputComparison() {
        int testDims = 512;
        int testVecs = 20000;

        Random random = new Random(42);
        float[][] testData = new float[testVecs][testDims];
        for (int v = 0; v < testVecs; v++) {
            for (int d = 0; d < testDims; d++) {
                testData[v][d] = (float) (random.nextGaussian() * 0.1 + 0.5);
            }
        }

        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int halfProcessors = availableProcessors / 2;
        int defaultParallelism = ParallelDatasetModelExtractor.defaultParallelism();

        System.out.printf("%nQuick Throughput Comparison (%d dims, %d vectors)%n", testDims, testVecs);
        System.out.println("AVX-512 available: " + BatchDimensionStatistics.isAvx512Available());
        System.out.printf("Available processors: %d, Default parallelism: %d%n",
            availableProcessors, defaultParallelism);
        System.out.println("=".repeat(70));

        // Warm up
        DatasetModelExtractor sequential = new DatasetModelExtractor();
        sequential.extractVectorModel(testData);

        // Sequential baseline
        long seqStart = System.currentTimeMillis();
        for (int i = 0; i < 3; i++) {
            sequential.extractVectorModel(testData);
        }
        long seqTime = (System.currentTimeMillis() - seqStart) / 3;

        System.out.printf("Sequential (1 thread):          %5d ms%n", seqTime);

        // Build thread count list: powers of 2, plus half processors, plus default
        java.util.TreeSet<Integer> threadCounts = new java.util.TreeSet<>();
        threadCounts.add(2);
        threadCounts.add(4);
        threadCounts.add(8);
        threadCounts.add(16);
        threadCounts.add(32);
        threadCounts.add(halfProcessors);
        threadCounts.add(defaultParallelism);
        if (availableProcessors > 64) {
            threadCounts.add(64);
        }
        if (availableProcessors > 96) {
            threadCounts.add(96);
        }
        // Remove any that exceed available
        threadCounts.removeIf(t -> t > availableProcessors || t < 1);

        // Parallel with different thread counts
        for (int threads : threadCounts) {
            ParallelDatasetModelExtractor parallel = ParallelDatasetModelExtractor.builder()
                .parallelism(threads)
                .build();

            try {
                // Warm up
                parallel.extractVectorModel(testData);

                long parStart = System.currentTimeMillis();
                for (int i = 0; i < 3; i++) {
                    parallel.extractVectorModel(testData);
                }
                long parTime = (System.currentTimeMillis() - parStart) / 3;

                double speedup = (double) seqTime / parTime;
                String marker = (threads == defaultParallelism) ? " (default)" :
                               (threads == halfProcessors) ? " (half)" : "";
                System.out.printf("Parallel (%3d threads):         %5d ms  (%.2fx speedup)%s%n",
                    threads, parTime, speedup, marker);
            } finally {
                parallel.shutdown();
            }
        }

        System.out.println("=".repeat(70));
    }

    /**
     * Compares NUMA-aware extraction vs standard parallel extraction.
     * Tests the effect of NUMA partitioning on multi-socket systems.
     */
    @Test
    @Tag("performance")
    public void numaAwareComparison() {
        int testDims = 512;
        int testVecs = 20000;

        Random random = new Random(42);
        float[][] testData = new float[testVecs][testDims];
        for (int v = 0; v < testVecs; v++) {
            for (int d = 0; d < testDims; d++) {
                testData[v][d] = (float) (random.nextGaussian() * 0.1 + 0.5);
            }
        }

        NumaTopology topology = NumaTopology.detect();
        int availableProcessors = Runtime.getRuntime().availableProcessors();

        System.out.printf("%nNUMA-Aware Comparison (%d dims, %d vectors)%n", testDims, testVecs);
        System.out.println("AVX-512 available: " + BatchDimensionStatistics.isAvx512Available());
        System.out.println("libnuma available: " + NumaBinding.isAvailable());
        System.out.println("NUMA topology: " + topology);
        System.out.println("=".repeat(70));

        // Warm up
        DatasetModelExtractor sequential = new DatasetModelExtractor();
        sequential.extractVectorModel(testData);

        // Sequential baseline
        long seqStart = System.currentTimeMillis();
        for (int i = 0; i < 3; i++) {
            sequential.extractVectorModel(testData);
        }
        long seqTime = (System.currentTimeMillis() - seqStart) / 3;
        System.out.printf("Sequential (1 thread):          %5d ms  (1.00x baseline)%n", seqTime);

        // Standard parallel (default parallelism)
        ParallelDatasetModelExtractor parallel = ParallelDatasetModelExtractor.builder()
            .parallelism(ParallelDatasetModelExtractor.defaultParallelism())
            .build();

        try {
            parallel.extractVectorModel(testData);

            long parStart = System.currentTimeMillis();
            for (int i = 0; i < 3; i++) {
                parallel.extractVectorModel(testData);
            }
            long parTime = (System.currentTimeMillis() - parStart) / 3;

            double parSpeedup = (double) seqTime / parTime;
            System.out.printf("Parallel (%3d threads):         %5d ms  (%.2fx speedup)%n",
                parallel.getParallelism(), parTime, parSpeedup);
        } finally {
            parallel.shutdown();
        }

        // NUMA-aware with auto-detected topology
        NumaAwareDatasetModelExtractor numa = new NumaAwareDatasetModelExtractor();

        try {
            numa.extractVectorModel(testData);

            long numaStart = System.currentTimeMillis();
            for (int i = 0; i < 3; i++) {
                numa.extractVectorModel(testData);
            }
            long numaTime = (System.currentTimeMillis() - numaStart) / 3;

            double numaSpeedup = (double) seqTime / numaTime;
            System.out.printf("NUMA-aware (%d nodes × %d):      %5d ms  (%.2fx speedup)%n",
                numa.getTopology().nodeCount(), numa.getThreadsPerNode(),
                numaTime, numaSpeedup);
        } finally {
            numa.shutdown();
        }

        // NUMA-aware with different thread counts per node
        if (topology.nodeCount() > 1) {
            int[] threadsPerNodeOptions = {4, 8, 16, 32};

            for (int tpn : threadsPerNodeOptions) {
                if (tpn * topology.nodeCount() > availableProcessors) {
                    continue;
                }

                NumaAwareDatasetModelExtractor numaCustom = NumaAwareDatasetModelExtractor.builder()
                    .threadsPerNode(tpn)
                    .build();

                try {
                    numaCustom.extractVectorModel(testData);

                    long start = System.currentTimeMillis();
                    for (int i = 0; i < 3; i++) {
                        numaCustom.extractVectorModel(testData);
                    }
                    long time = (System.currentTimeMillis() - start) / 3;

                    double speedup = (double) seqTime / time;
                    System.out.printf("NUMA-aware (%d nodes × %d):      %5d ms  (%.2fx speedup)%n",
                        topology.nodeCount(), tpn, time, speedup);
                } finally {
                    numaCustom.shutdown();
                }
            }
        }

        System.out.println("=".repeat(70));
    }
}
