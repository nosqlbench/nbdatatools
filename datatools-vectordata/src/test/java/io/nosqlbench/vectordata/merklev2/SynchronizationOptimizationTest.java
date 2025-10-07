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

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

/// Test to compare performance between original and optimized synchronization approaches.
/// 
/// This test measures the impact of synchronization optimizations on:
/// 1. ChunkQueue scheduling throughput
/// 2. Concurrent access patterns
/// 3. Scalability with increasing thread counts
/// 
/// The goal is to validate that optimizations provide measurable improvements
/// without breaking correctness.
public class SynchronizationOptimizationTest {
    
    private static final int OPERATIONS_PER_THREAD = 10000;
    private static final int[] THREAD_COUNTS = {1, 2, 4, 8, 16, 32};
    
    /// Compares ChunkQueue vs OptimizedChunkQueue performance under concurrent load.
    @Test
    void compareChunkQueuePerformance() throws Exception {
        System.out.println("\n=== ChunkQueue Performance Comparison ===");
        System.out.println("Operations per thread: " + OPERATIONS_PER_THREAD);
        System.out.println();
        
        for (int threadCount : THREAD_COUNTS) {
            System.out.printf("Threads: %2d\n", threadCount);
            
            // Test original ChunkQueue
            long originalTime = measureChunkQueuePerformance(
                new ChunkQueue(10000), threadCount, "Original"
            );
            
            // Test optimized ChunkQueue  
            long optimizedTime = measureOptimizedChunkQueuePerformance(
                new OptimizedChunkQueue(10000), threadCount, "Optimized"
            );
            
            double improvement = (double) originalTime / optimizedTime;
            System.out.printf("  Improvement: %.2fx\n", improvement);
            System.out.println();
        }
    }
    
    /// Measures performance of original ChunkQueue under concurrent scheduling.
    private long measureChunkQueuePerformance(ChunkQueue chunkQueue, int threadCount, String label) 
            throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicLong totalTime = new AtomicLong(0);
        AtomicInteger completedOperations = new AtomicInteger(0);
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    
                    long threadStartTime = System.nanoTime();
                    
                    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                        // Simulate scheduling operation
                        chunkQueue.executeScheduling(wrapper -> {
                            for (int j = 0; j < 3; j++) {
                                MockNodeDownloadTask task = new MockNodeDownloadTask(j, j * 1024, 1024);
                                wrapper.offerTask(task);
                            }
                        });
                        completedOperations.incrementAndGet();
                    }
                    
                    long threadEndTime = System.nanoTime();
                    totalTime.addAndGet(threadEndTime - threadStartTime);
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        long measurementStart = System.nanoTime();
        startLatch.countDown();
        endLatch.await(60, TimeUnit.SECONDS);
        long measurementEnd = System.nanoTime();
        
        executor.shutdown();
        
        long totalDuration = measurementEnd - measurementStart;
        long avgPerOperation = totalTime.get() / completedOperations.get();
        
        System.out.printf("  %s - Total: %6d ms, Avg per op: %6d ns\n", 
                         label, totalDuration / 1_000_000, avgPerOperation);
        
        return totalDuration;
    }
    
    /// Measures performance of OptimizedChunkQueue under concurrent scheduling.
    private long measureOptimizedChunkQueuePerformance(OptimizedChunkQueue chunkQueue, int threadCount, String label) 
            throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicLong totalTime = new AtomicLong(0);
        AtomicInteger completedOperations = new AtomicInteger(0);
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    
                    long threadStartTime = System.nanoTime();
                    
                    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                        // Simulate scheduling operation
                        chunkQueue.executeScheduling(wrapper -> {
                            for (int j = 0; j < 3; j++) {
                                MockNodeDownloadTask task = new MockNodeDownloadTask(j, j * 1024, 1024);
                                wrapper.offerTask(task);
                            }
                        });
                        completedOperations.incrementAndGet();
                    }
                    
                    long threadEndTime = System.nanoTime();
                    totalTime.addAndGet(threadEndTime - threadStartTime);
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        long measurementStart = System.nanoTime();
        startLatch.countDown();
        endLatch.await(60, TimeUnit.SECONDS);
        long measurementEnd = System.nanoTime();
        
        executor.shutdown();
        
        long totalDuration = measurementEnd - measurementStart;
        long avgPerOperation = totalTime.get() / completedOperations.get();
        
        System.out.printf("  %s - Total: %6d ms, Avg per op: %6d ns\n", 
                         label, totalDuration / 1_000_000, avgPerOperation);
        
        return totalDuration;
    }
    
    /// Tests the correctness of optimizations under concurrent access.
    @Test
    void testOptimizationCorrectness() throws Exception {
        System.out.println("\n=== Optimization Correctness Test ===");
        
        OptimizedChunkQueue optimizedQueue = new OptimizedChunkQueue(1000);
        int threadCount = 16;
        int operationsPerThread = 1000;
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger totalTasksAdded = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);
        
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    
                    for (int i = 0; i < operationsPerThread; i++) {
                        int nodeIndex = threadId * operationsPerThread + i;
                        
                        // Test scheduling operations
                        OptimizedChunkQueue.SchedulingResult result = optimizedQueue.executeSchedulingWithTasks(wrapper -> {
                            MockNodeDownloadTask task = new MockNodeDownloadTask(nodeIndex, nodeIndex * 1024, 1024);
                            wrapper.offerTask(task);
                        });
                        
                        if (result.tasks().size() == 1 && result.futures().size() == 1) {
                            totalTasksAdded.incrementAndGet();
                        } else {
                            errors.incrementAndGet();
                        }
                        
                        // Test concurrent future operations
                        CompletableFuture<Void> future1 = optimizedQueue.getOrCreateFuture(nodeIndex);
                        CompletableFuture<Void> future2 = optimizedQueue.getOrCreateFuture(nodeIndex);
                        
                        if (future1 != future2) {
                            errors.incrementAndGet(); // Should be the same future
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        startLatch.countDown();
        endLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        
        int expectedTasks = threadCount * operationsPerThread;
        
        System.out.printf("Expected tasks: %d\n", expectedTasks);
        System.out.printf("Actual tasks added: %d\n", totalTasksAdded.get());
        System.out.printf("Errors: %d\n", errors.get());
        System.out.printf("Pending tasks: %d\n", optimizedQueue.getPendingTaskCount());
        System.out.printf("In-flight futures: %d\n", optimizedQueue.getInFlightCount());
        
        if (errors.get() > 0) {
            throw new AssertionError("Optimization correctness test failed with " + errors.get() + " errors");
        }
        
        if (totalTasksAdded.get() != expectedTasks) {
            throw new AssertionError("Expected " + expectedTasks + " tasks but got " + totalTasksAdded.get());
        }
        
        System.out.println("✓ Optimization correctness test passed");
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