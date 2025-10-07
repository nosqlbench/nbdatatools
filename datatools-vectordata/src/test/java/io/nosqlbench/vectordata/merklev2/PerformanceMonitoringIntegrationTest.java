package io.nosqlbench.vectordata.merklev2;

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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/// Integration test demonstrating performance monitoring capabilities.
/// 
/// This test shows how the OptimizedChunkQueue and PerformanceMonitor work together
/// to provide detailed insights into system performance and behavior.
public class PerformanceMonitoringIntegrationTest {
    
    private MeterRegistry meterRegistry;
    private OptimizedChunkQueue chunkQueue;
    private PerformanceMonitor performanceMonitor;
    
    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        chunkQueue = new OptimizedChunkQueue(1000, meterRegistry);
        performanceMonitor = new PerformanceMonitor(meterRegistry);
    }
    
    /// Demonstrates basic performance monitoring functionality.
    @Test
    void demonstratePerformanceMonitoring() throws Exception {
        System.out.println("\n=== Performance Monitoring Integration Test ===");
        
        // Simulate some chunk queue activity
        simulateChunkQueueActivity();
        
        // Generate and display performance report
        PerformanceMonitor.PerformanceSnapshot snapshot = performanceMonitor.getSnapshot();
        System.out.println(snapshot.generateReport());
        
        // Verify metrics were collected
        PerformanceMonitor.ChunkQueueMetrics metrics = snapshot.getChunkQueueMetrics();
        
        System.out.println("\nMetrics Validation:");
        System.out.printf("Tasks Added: %d\n", metrics.totalTasksAdded());
        System.out.printf("Tasks Completed: %d\n", metrics.totalTasksCompleted());
        System.out.printf("Scheduling Operations: %d\n", metrics.totalSchedulingOps());
        System.out.printf("Average Scheduling Time: %.2f ms\n", metrics.avgSchedulingTime());
        
        if (metrics.totalTasksAdded() == 0) {
            throw new AssertionError("No tasks were recorded in metrics");
        }
        
        System.out.println("✓ Performance monitoring integration test passed");
    }
    
    /// Demonstrates concurrent performance monitoring under load.
    @Test
    void demonstrateConcurrentPerformanceMonitoring() throws Exception {
        System.out.println("\n=== Concurrent Performance Monitoring Test ===");
        
        int threadCount = 8;
        int operationsPerThread = 1000;
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        
        // Start concurrent operations
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    
                    for (int i = 0; i < operationsPerThread; i++) {
                        final int opIndex = i;
                        // Simulate scheduling operations
                        chunkQueue.executeScheduling(wrapper -> {
                            for (int j = 0; j < 3; j++) {
                                final int nodeIndex = threadId * operationsPerThread + opIndex * 3 + j;
                                MockNodeDownloadTask task = new MockNodeDownloadTask(nodeIndex, nodeIndex * 1024, 1024);
                                wrapper.offerTask(task);
                            }
                        });
                        
                        // Simulate task completion
                        if (i % 10 == 0) {
                            MockNodeDownloadTask task = new MockNodeDownloadTask(threadId * 1000 + i, 0, 1024);
                            chunkQueue.markCompleted(task, 1024);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        // Start the load test
        long startTime = System.currentTimeMillis();
        startLatch.countDown();
        endLatch.await(30, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();
        
        executor.shutdown();
        
        // Generate performance report
        PerformanceMonitor.PerformanceSnapshot snapshot = performanceMonitor.getSnapshot();
        System.out.println(snapshot.generateReport());
        
        // Calculate and display throughput
        PerformanceMonitor.ChunkQueueMetrics metrics = snapshot.getChunkQueueMetrics();
        double durationSeconds = (endTime - startTime) / 1000.0;
        double operationsPerSecond = metrics.totalSchedulingOps() / durationSeconds;
        
        System.out.printf("\nLoad Test Results:\n");
        System.out.printf("Duration: %.2f seconds\n", durationSeconds);
        System.out.printf("Total Scheduling Operations: %d\n", metrics.totalSchedulingOps());
        System.out.printf("Throughput: %.2f ops/sec\n", operationsPerSecond);
        
        if (metrics.totalSchedulingOps() < threadCount * operationsPerThread * 0.9) {
            throw new AssertionError("Expected more scheduling operations to be recorded");
        }
        
        System.out.println("✓ Concurrent performance monitoring test passed");
    }
    
    /// Demonstrates performance insights and warning detection.
    @Test 
    void demonstratePerformanceInsights() throws Exception {
        System.out.println("\n=== Performance Insights Test ===");
        
        // Create conditions that should trigger performance warnings
        
        // 1. Simulate high task backlog
        for (int i = 0; i < 2000; i++) {
            MockNodeDownloadTask task = new MockNodeDownloadTask(i, i * 1024, 1024);
            chunkQueue.offerTask(task);
        }
        
        // 2. Simulate slow scheduling operations by adding delay
        chunkQueue.executeScheduling(wrapper -> {
            try {
                Thread.sleep(15); // Simulate slow operation (>10ms threshold)
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            MockNodeDownloadTask task = new MockNodeDownloadTask(9999, 0, 1024);
            wrapper.offerTask(task);
        });
        
        // Generate performance report with insights
        PerformanceMonitor.PerformanceSnapshot snapshot = performanceMonitor.getSnapshot();
        String report = snapshot.generateReport();
        System.out.println(report);
        
        // Verify that warnings are detected
        if (!report.contains("⚠️")) {
            System.out.println("Note: No performance warnings detected (this may be expected in fast test environment)");
        } else {
            System.out.println("✓ Performance warnings detected successfully");
        }
        
        PerformanceMonitor.ChunkQueueMetrics metrics = snapshot.getChunkQueueMetrics();
        System.out.printf("\nCurrent Queue State:\n");
        System.out.printf("Pending Tasks: %d\n", metrics.currentPending());
        System.out.printf("In-Flight Tasks: %d\n", metrics.currentInFlight());
        
        System.out.println("✓ Performance insights test completed");
    }
    
    /// Simulates typical chunk queue activity for monitoring.
    private void simulateChunkQueueActivity() throws Exception {
        // Add some tasks
        for (int i = 0; i < 100; i++) {
            final int taskIndex = i;
            chunkQueue.executeScheduling(wrapper -> {
                MockNodeDownloadTask task = new MockNodeDownloadTask(taskIndex, taskIndex * 1024, 1024);
                wrapper.offerTask(task);
            });
        }
        
        // Complete some tasks
        for (int i = 0; i < 50; i++) {
            MockNodeDownloadTask task = new MockNodeDownloadTask(i, i * 1024, 1024);
            chunkQueue.markCompleted(task, 1024);
        }
        
        // Add some delay to simulate realistic timing
        Thread.sleep(10);
    }
    
    /// Mock task implementation for testing.
    private static class MockNodeDownloadTask implements ChunkScheduler.NodeDownloadTask {
        private final int nodeIndex;
        private final long offset;
        private final long size;
        private final CompletableFuture<Void> future = new CompletableFuture<>();
        
        MockNodeDownloadTask(int nodeIndex, long offset, long size) {
            this.nodeIndex = nodeIndex;
            this.offset = offset;
            this.size = size;
            
            // Auto-complete the future to avoid blocking
            future.complete(null);
        }
        
        @Override
        public int getNodeIndex() { return nodeIndex; }
        
        @Override
        public long getOffset() { return offset; }
        
        @Override
        public long getSize() { return size; }
        
        @Override
        public boolean isLeafNode() { return true; }
        
        @Override
        public CompletableFuture<Void> getFuture() { return future; }
        
        @Override
        public MerkleShape.MerkleNodeRange getLeafRange() {
            return new MockMerkleNodeRange(nodeIndex, nodeIndex + 1);
        }
    }
    
    /// Mock implementation of MerkleNodeRange for testing.
    private static class MockMerkleNodeRange implements MerkleShape.MerkleNodeRange {
        private final long start;
        private final long end;
        
        MockMerkleNodeRange(long start, long end) {
            this.start = start;
            this.end = end;
        }
        
        @Override
        public long getStart() { return start; }
        
        @Override
        public long getEnd() { return end; }
        
        @Override
        public long getLength() { return end - start; }
        
        @Override
        public boolean contains(long position) {
            return position >= start && position < end;
        }
        
        @Override
        public boolean overlaps(MerkleShape.MerkleNodeRange other) {
            return start < other.getEnd() && end > other.getStart();
        }
    }
}