package io.nosqlbench.vectordata.merkle;

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

import io.nosqlbench.vectordata.fvecs.LargeFvecsFileGenerationTest;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Test class specifically for measuring the performance of MerkleTree file loading.
 * This test uses the same file that the MerkleTreePerformanceTest uses.
 */
@Tag("performance")
@Disabled
public class MerkleTreeLoadingPerformanceTest {

    private static final int WARMUP_ITERATIONS = 3;
    private static final int BENCHMARK_ITERATIONS = 5;

    /**
     * Test method that measures the performance of loading a MerkleTree file.
     * This test is skipped if the merkle file doesn't exist.
     *
     * @throws IOException If there's an error loading the MerkleTree
     */
    @Tag("performance")
    @Test
    public void testMerkleTreeLoadingPerformance() throws IOException {
        // Initialize profiling tools
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        // Enable CPU time measurement if supported
        if (threadMXBean.isCurrentThreadCpuTimeSupported()) {
            threadMXBean.setThreadCpuTimeEnabled(true);
        }

        // Get initial memory usage
        MemoryUsage heapBefore = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeapBefore = memoryMXBean.getNonHeapMemoryUsage();

        // Get initial CPU time
        long startCpuTime = threadMXBean.isCurrentThreadCpuTimeSupported() ?
            threadMXBean.getCurrentThreadCpuTime() : 0;

        System.out.println("[DEBUG_LOG] ===== PROFILING: Starting MerkleTree Loading Performance Test =====");
        System.out.println("[DEBUG_LOG] Initial Heap Memory: " + formatMemorySize(heapBefore.getUsed()) +
            " / " + formatMemorySize(heapBefore.getCommitted()));
        System.out.println("[DEBUG_LOG] Initial Non-Heap Memory: " + formatMemorySize(nonHeapBefore.getUsed()) +
            " / " + formatMemorySize(nonHeapBefore.getCommitted()));

        // Ensure the large fvecs file exists by calling the helper method
        Path largeFvecsFilePath = LargeFvecsFileGenerationTest.ensureLargeFvecsFileExists();

        // Skip the test if the large fvecs file doesn't exist
        Assumptions.assumeTrue(Files.exists(largeFvecsFilePath),
            "Skipping test because the large fvecs file couldn't be created at " + largeFvecsFilePath);

        // Construct the path to the merkle file
        Path merkleFilePath = largeFvecsFilePath.resolveSibling(largeFvecsFilePath.getFileName() + ".mrkl");

        // Skip the test if the merkle file doesn't exist
        Assumptions.assumeTrue(Files.exists(merkleFilePath),
            "Skipping test because the merkle file doesn't exist at " + merkleFilePath + ". Run MerkleTreePerformanceTest first.");

        System.out.println("[DEBUG_LOG] Starting MerkleTree loading performance test");
        System.out.println("[DEBUG_LOG] Merkle file: " + merkleFilePath);
        System.out.println("[DEBUG_LOG] File size: " + (Files.size(merkleFilePath) / (1024 * 1024)) + " MB");

        // Warm up the JVM
        System.out.println("[DEBUG_LOG] Warming up JVM with " + WARMUP_ITERATIONS + " iterations...");
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            // Note: The verify parameter is kept for backward compatibility but is ignored in production code
            try (MerkleTree warmupTree = MerkleTree.load(merkleFilePath, false)) {
                System.out.println("[DEBUG_LOG] Warmup iteration " + (i + 1) + " completed");
            }
        }

        // Benchmark loading
        System.out.println("[DEBUG_LOG] Benchmarking load time (" + BENCHMARK_ITERATIONS + " iterations)...");
        List<Long> loadTimes = new ArrayList<>();
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            long startTime = System.nanoTime();
            try (MerkleTree tree = MerkleTree.load(merkleFilePath, false)) {
                long endTime = System.nanoTime();
                long duration = endTime - startTime;
                loadTimes.add(duration);
                System.out.println("[DEBUG_LOG] Iteration " + (i + 1) + ": " + (duration / 1_000_000.0) + " ms");
            }
        }

        // Calculate and print statistics
        double avg = calculateAverage(loadTimes) / 1_000_000.0;
        double min = calculateMin(loadTimes) / 1_000_000.0;
        double max = calculateMax(loadTimes) / 1_000_000.0;
        double stdDev = calculateStdDev(loadTimes, avg * 1_000_000) / 1_000_000.0;

        System.out.println("\n[DEBUG_LOG] ===== MerkleTree Loading Performance Results =====");
        System.out.println("[DEBUG_LOG] File: " + merkleFilePath);
        System.out.println("[DEBUG_LOG] File size: " + (Files.size(merkleFilePath) / (1024 * 1024)) + " MB");
        System.out.println("[DEBUG_LOG] Iterations: " + BENCHMARK_ITERATIONS);
        System.out.println("[DEBUG_LOG]   Average: " + String.format("%.2f", avg) + " ms");
        System.out.println("[DEBUG_LOG]   Min: " + String.format("%.2f", min) + " ms");
        System.out.println("[DEBUG_LOG]   Max: " + String.format("%.2f", max) + " ms");
        System.out.println("[DEBUG_LOG]   StdDev: " + String.format("%.2f", stdDev) + " ms");

        // Get final memory usage
        MemoryUsage heapAfter = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeapAfter = memoryMXBean.getNonHeapMemoryUsage();

        // Get final CPU time
        long endCpuTime = threadMXBean.isCurrentThreadCpuTimeSupported() ?
            threadMXBean.getCurrentThreadCpuTime() : 0;

        // Calculate memory and CPU usage
        long heapUsed = heapAfter.getUsed() - heapBefore.getUsed();
        long nonHeapUsed = nonHeapAfter.getUsed() - nonHeapBefore.getUsed();
        long cpuTimeNanos = endCpuTime - startCpuTime;

        // Print profiling summary
        System.out.println("\n[DEBUG_LOG] ===== PROFILING: MerkleTree Loading Performance Summary =====");
        System.out.println("[DEBUG_LOG] Heap Memory Used: " + formatMemorySize(heapUsed) +
            " (from " + formatMemorySize(heapBefore.getUsed()) + " to " + formatMemorySize(heapAfter.getUsed()) + ")");
        System.out.println("[DEBUG_LOG] Non-Heap Memory Used: " + formatMemorySize(nonHeapUsed) +
            " (from " + formatMemorySize(nonHeapBefore.getUsed()) + " to " + formatMemorySize(nonHeapAfter.getUsed()) + ")");

        if (threadMXBean.isCurrentThreadCpuTimeSupported()) {
            double cpuTimeMs = cpuTimeNanos / 1_000_000.0;
            System.out.println("[DEBUG_LOG] CPU Time: " + String.format("%.2f ms", cpuTimeMs));
        }

        System.out.println("[DEBUG_LOG] ===== PROFILING: End of Summary =====");
        System.out.println("[DEBUG_LOG] MerkleTree loading performance test completed");
    }

    /**
     * Formats a memory size in bytes to a human-readable string.
     *
     * @param bytes The memory size in bytes
     * @return A human-readable string representation of the memory size
     */
    private String formatMemorySize(long bytes) {
        final long kilobyte = 1024;
        final long megabyte = kilobyte * 1024;
        final long gigabyte = megabyte * 1024;

        if (bytes >= gigabyte) {
            return String.format("%.2f GB", (double) bytes / gigabyte);
        } else if (bytes >= megabyte) {
            return String.format("%.2f MB", (double) bytes / megabyte);
        } else if (bytes >= kilobyte) {
            return String.format("%.2f KB", (double) bytes / kilobyte);
        } else {
            return bytes + " bytes";
        }
    }

    /**
     * Calculates the average of a list of long values.
     *
     * @param values The list of values
     * @return The average value
     */
    private double calculateAverage(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).average().orElse(0);
    }

    /**
     * Calculates the minimum of a list of long values.
     *
     * @param values The list of values
     * @return The minimum value
     */
    private long calculateMin(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).min().orElse(0);
    }

    /**
     * Calculates the maximum of a list of long values.
     *
     * @param values The list of values
     * @return The maximum value
     */
    private long calculateMax(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).max().orElse(0);
    }

    /**
     * Calculates the standard deviation of a list of long values.
     *
     * @param values The list of values
     * @param mean The mean value
     * @return The standard deviation
     */
    private double calculateStdDev(List<Long> values, double mean) {
        double variance = values.stream()
            .mapToDouble(value -> Math.pow(value - mean, 2))
            .average()
            .orElse(0);
        return Math.sqrt(variance);
    }
}
