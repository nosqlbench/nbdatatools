package io.nosqlbench.vectordata.simulation.mockdriven;

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

import io.nosqlbench.vectordata.merklev2.ChunkScheduler;
import io.nosqlbench.vectordata.merklev2.ChunkQueue;
import io.nosqlbench.vectordata.merklev2.MerkleShape;
import io.nosqlbench.vectordata.merklev2.MerkleState;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/// Framework for testing and comparing scheduler performance under various conditions.
/// 
/// This class provides a comprehensive testing environment for evaluating different
/// chunk download scheduling strategies. It can simulate various network conditions,
/// file sizes, and access patterns to determine which scheduler performs best
/// under specific circumstances.
/// 
/// Key features:
/// - Multiple scheduler comparison in single test run
/// - Configurable test scenarios (file size, chunk patterns, network conditions)
/// - Detailed performance metrics and analysis
/// - Statistical validation across multiple runs
/// - Support for custom test workloads
/// 
/// Example usage:
/// ```java
/// SchedulerPerformanceTest test = new SchedulerPerformanceTest()
///     .withFileSize(1_000_000_000L) // 1GB
///     .withChunkSize(1_048_576) // 1MB chunks
///     .withNetworkConditions(NetworkConditions.Scenarios.BROADBAND_FAST)
///     .withSchedulers(
///         new DefaultChunkScheduler(),
///         new AggressiveScheduler(),
///         new ConservativeScheduler()
///     )
///     .withWorkload(SchedulerWorkload.SEQUENTIAL_READ)
///     .withIterations(5);
/// 
/// PerformanceResults results = test.run();
/// results.printSummary();
/// ```
public class SchedulerPerformanceTest {
    
    /// Default test parameters
    private static final long DEFAULT_FILE_SIZE = 100_000_000L; // 100MB
    private static final int DEFAULT_CHUNK_SIZE = 1024 * 1024; // 1MB
    private static final int DEFAULT_ITERATIONS = 3;
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);
    
    /// Test configuration
    private long fileSize = DEFAULT_FILE_SIZE;
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    private List<NetworkConditions> networkConditions = Arrays.asList(NetworkConditions.Scenarios.BROADBAND_FAST);
    private List<ChunkScheduler> schedulers = new ArrayList<>();
    private List<SchedulerWorkload> workloads = Arrays.asList(SchedulerWorkload.SEQUENTIAL_READ);
    private int iterations = DEFAULT_ITERATIONS;
    private Duration timeout = DEFAULT_TIMEOUT;
    private double initialStateRatio = 0.0; // Start with empty state
    private long randomSeed = 42L;
    
    /// Sets the file size for testing.
    /// 
    /// @param fileSize File size in bytes
    /// @return This test instance for method chaining
    public SchedulerPerformanceTest withFileSize(long fileSize) {
        if (fileSize <= 0) {
            throw new IllegalArgumentException("File size must be positive");
        }
        this.fileSize = fileSize;
        return this;
    }
    
    /// Sets the chunk size for testing.
    /// 
    /// @param chunkSize Chunk size in bytes
    /// @return This test instance for method chaining
    public SchedulerPerformanceTest withChunkSize(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }
        this.chunkSize = chunkSize;
        return this;
    }
    
    /// Sets the network conditions to test under.
    /// 
    /// @param conditions Network conditions to use
    /// @return This test instance for method chaining
    public SchedulerPerformanceTest withNetworkConditions(NetworkConditions... conditions) {
        this.networkConditions = Arrays.asList(conditions);
        return this;
    }
    
    /// Sets the network conditions to test under.
    /// 
    /// @param conditions List of network conditions to use
    /// @return This test instance for method chaining
    public SchedulerPerformanceTest withNetworkConditions(List<NetworkConditions> conditions) {
        this.networkConditions = new ArrayList<>(conditions);
        return this;
    }
    
    /// Sets the schedulers to compare.
    /// 
    /// @param schedulers Schedulers to test
    /// @return This test instance for method chaining
    public SchedulerPerformanceTest withSchedulers(ChunkScheduler... schedulers) {
        this.schedulers = Arrays.asList(schedulers);
        return this;
    }
    
    /// Sets the schedulers to compare.
    /// 
    /// @param schedulers List of schedulers to test
    /// @return This test instance for method chaining
    public SchedulerPerformanceTest withSchedulers(List<ChunkScheduler> schedulers) {
        this.schedulers = new ArrayList<>(schedulers);
        return this;
    }
    
    /// Sets the workloads to test.
    /// 
    /// @param workloads Workloads to test
    /// @return This test instance for method chaining
    public SchedulerPerformanceTest withWorkloads(SchedulerWorkload... workloads) {
        this.workloads = Arrays.asList(workloads);
        return this;
    }
    
    /// Sets the number of iterations for statistical validation.
    /// 
    /// @param iterations Number of test iterations
    /// @return This test instance for method chaining
    public SchedulerPerformanceTest withIterations(int iterations) {
        if (iterations <= 0) {
            throw new IllegalArgumentException("Iterations must be positive");
        }
        this.iterations = iterations;
        return this;
    }
    
    /// Sets the timeout for individual test runs.
    /// 
    /// @param timeout Maximum time per test run
    /// @return This test instance for method chaining
    public SchedulerPerformanceTest withTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }
    
    /// Sets the initial state ratio for partial download scenarios.
    /// 
    /// @param ratio Ratio of chunks initially valid (0.0 to 1.0)
    /// @return This test instance for method chaining
    public SchedulerPerformanceTest withInitialStateRatio(double ratio) {
        if (ratio < 0.0 || ratio > 1.0) {
            throw new IllegalArgumentException("Initial state ratio must be between 0.0 and 1.0");
        }
        this.initialStateRatio = ratio;
        return this;
    }
    
    /// Sets the random seed for reproducible tests.
    /// 
    /// @param seed Random seed
    /// @return This test instance for method chaining
    public SchedulerPerformanceTest withRandomSeed(long seed) {
        this.randomSeed = seed;
        return this;
    }
    
    /// Runs the performance test with all configured parameters.
    /// 
    /// @return Comprehensive results from all test combinations
    /// @throws InterruptedException If the test is interrupted
    /// @throws ExecutionException If a test execution fails
    public PerformanceResults run() throws InterruptedException, ExecutionException {
        if (schedulers.isEmpty()) {
            throw new IllegalStateException("No schedulers configured for testing");
        }
        
        System.out.println("Starting scheduler performance test...");
        System.out.printf("File size: %d bytes, Chunk size: %d bytes%n", fileSize, chunkSize);
        System.out.printf("Network conditions: %d scenarios%n", networkConditions.size());
        System.out.printf("Schedulers: %d implementations%n", schedulers.size());
        System.out.printf("Workloads: %d patterns%n", workloads.size());
        System.out.printf("Iterations: %d per combination%n", iterations);
        
        Map<TestCombination, List<SingleTestResult>> allResults = new HashMap<>();
        
        // Test all combinations
        for (NetworkConditions network : networkConditions) {
            for (ChunkScheduler scheduler : schedulers) {
                for (SchedulerWorkload workload : workloads) {
                    TestCombination combination = new TestCombination(
                        scheduler.getClass().getSimpleName(),
                        network.getDescription(),
                        workload.getName()
                    );
                    
                    System.out.printf("Testing: %s%n", combination);
                    
                    List<SingleTestResult> results = runTestCombination(
                        scheduler, network, workload
                    );
                    
                    allResults.put(combination, results);
                }
            }
        }
        
        return new PerformanceResults(allResults);
    }
    
    /// Runs a specific test combination multiple times.
    private List<SingleTestResult> runTestCombination(
            ChunkScheduler scheduler,
            NetworkConditions networkConditions,
            SchedulerWorkload workload) throws InterruptedException, ExecutionException {
        
        List<SingleTestResult> results = new ArrayList<>();
        
        for (int iteration = 0; iteration < iterations; iteration++) {
            SingleTestResult result = runSingleTest(scheduler, networkConditions, workload, iteration);
            results.add(result);
            
            // Brief pause between iterations
            Thread.sleep(100);
        }
        
        return results;
    }
    
    /// Runs a single test iteration.
    private SingleTestResult runSingleTest(
            ChunkScheduler scheduler,    
            NetworkConditions networkConditions,
            SchedulerWorkload workload,
            int iteration) throws InterruptedException, ExecutionException {
        
        // Create test environment
        SimulatedMerkleShape shape = new SimulatedMerkleShape(fileSize, chunkSize);
        SimulatedMerkleState state = SimulatedMerkleState.createPartialState(
            shape, initialStateRatio, randomSeed + iteration
        );
        
        MockChunkedTransportClient transport = new MockChunkedTransportClient(fileSize, networkConditions);
        
        try {
            // Execute the workload
            Instant startTime = Instant.now();
            WorkloadExecutionResult executionResult = executeWorkload(
                scheduler, shape, state, transport, workload
            );
            Instant endTime = Instant.now();
            
            Duration totalTime = Duration.between(startTime, endTime);
            TransportStatistics transportStats = transport.getStatistics();
            
            return new SingleTestResult(
                scheduler.getClass().getSimpleName(),
                networkConditions.getDescription(),
                workload.getName(),
                iteration,
                totalTime,
                executionResult,
                transportStats,
                state.getValidChunkCount(),
                shape.getTotalChunks()
            );
            
        } finally {
            try {
                transport.close();
            } catch (IOException e) {
                // Log but don't fail the test
                System.err.println("Error closing transport: " + e.getMessage());
            }
            state.close();
        }
    }
    
    /// Executes a specific workload using the given scheduler.
    private WorkloadExecutionResult executeWorkload(
            ChunkScheduler scheduler,
            MerkleShape shape,
            MerkleState state,
            MockChunkedTransportClient transport,
            SchedulerWorkload workload) throws InterruptedException, ExecutionException {
        
        // Create a ChunkQueue to manage tasks and state
        ChunkQueue chunkQueue = new ChunkQueue(1000);
        
        // Track execution metrics
        long totalBytesRequested = 0;
        int totalDownloads = 0;
        List<Duration> downloadTimes = new ArrayList<>();
        
        // Execute workload requests
        List<WorkloadRequest> requests = workload.generateRequests(shape);
        
        for (WorkloadRequest request : requests) {
            Instant requestStart = Instant.now();
            
            // Schedule downloads for this request using synchronized ChunkQueue
            List<CompletableFuture<Void>> futures = chunkQueue.executeScheduling(wrapper -> 
                scheduler.scheduleDownloads(request.getOffset(), request.getLength(), shape, state, wrapper)
            );
            
            // Process scheduled tasks (simulate task executor)
            while (!chunkQueue.isEmpty()) {
                ChunkScheduler.NodeDownloadTask task = chunkQueue.pollTask();
                if (task != null) {
                    // Simulate download execution
                    CompletableFuture<Void> downloadFuture = simulateDownload(
                        transport, state, shape, task
                    );
                    
                    totalBytesRequested += task.getSize();
                    totalDownloads++;
                }
            }
            
            // Wait for all downloads to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
            
            Instant requestEnd = Instant.now();
            downloadTimes.add(Duration.between(requestStart, requestEnd));
        }
        
        return new WorkloadExecutionResult(
            totalBytesRequested,
            totalDownloads,
            downloadTimes
        );
    }
    
    /// Simulates download execution for a task.
    private CompletableFuture<Void> simulateDownload(
            MockChunkedTransportClient transport,
            MerkleState state,
            MerkleShape shape,
            ChunkScheduler.NodeDownloadTask task) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Download the data
                var fetchResult = transport.fetchRange(task.getOffset(), (int) task.getSize()).get();
                
                // Simulate saving to state (for leaf nodes)
                if (task.isLeafNode()) {
                    int chunkIndex = shape.leafNodeToChunkIndex(task.getNodeIndex());
                    state.saveIfValid(chunkIndex, fetchResult.getData(), data -> {
                        // Simulate save callback
                    });
                }
                
                return null;
            } catch (Exception e) {
                throw new RuntimeException("Download failed", e);
            }
        });
    }
    
    /// Represents a single test combination.
    public static class TestCombination {
        private final String schedulerName;
        private final String networkCondition;
        private final String workloadName;
        
        public TestCombination(String schedulerName, String networkCondition, String workloadName) {
            this.schedulerName = schedulerName;
            this.networkCondition = networkCondition;
            this.workloadName = workloadName;
        }
        
        public String getSchedulerName() { return schedulerName; }
        public String getNetworkCondition() { return networkCondition; }
        public String getWorkloadName() { return workloadName; }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestCombination that = (TestCombination) o;
            return Objects.equals(schedulerName, that.schedulerName) &&
                   Objects.equals(networkCondition, that.networkCondition) &&
                   Objects.equals(workloadName, that.workloadName);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(schedulerName, networkCondition, workloadName);
        }
        
        @Override
        public String toString() {
            return String.format("%s on %s with %s", schedulerName, networkCondition, workloadName);
        }
    }
}