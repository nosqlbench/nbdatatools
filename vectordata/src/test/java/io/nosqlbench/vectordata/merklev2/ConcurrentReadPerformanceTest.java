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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/// Performance test to measure synchronization overhead and contention in MAFileChannel and ChunkQueue.
/// 
/// This test evaluates:
/// 1. Read lock contention in MAFileChannel during concurrent reads
/// 2. Synchronized method overhead in ChunkQueue during scheduling
/// 3. Impact of selective blocking vs all-blocking approaches
/// 4. Scalability with increasing thread counts
/// 
/// The test creates various scenarios to stress-test the synchronization mechanisms
/// and identify potential bottlenecks that could be optimized.
@Tag("performance")
public class ConcurrentReadPerformanceTest {
    
    private static final int SMALL_FILE_SIZE = 1024 * 1024; // 1MB
    private static final int MEDIUM_FILE_SIZE = 10 * 1024 * 1024; // 10MB  
    private static final int CHUNK_SIZE = 64 * 1024; // 64KB chunks
    private static final int WARMUP_ITERATIONS = 5;
    private static final int TEST_ITERATIONS = 10;
    
    private ExecutorService executorService;
    
    @BeforeEach
    void setUp() {
        executorService = ForkJoinPool.commonPool();
    }
    
    @AfterEach
    void tearDown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }
    
    /// Tests ChunkQueue synchronization overhead with concurrent scheduling operations.
    @Test
    void testChunkQueueSynchronizationOverhead() throws Exception {
        System.out.println("\n=== ChunkQueue Synchronization Overhead Test ===");
        
        ChunkQueue chunkQueue = new ChunkQueue(10000);
        
        // Test different thread counts
        int[] threadCounts = {1, 2, 4, 8, 16, 32};
        
        for (int threadCount : threadCounts) {
            long overhead = measureChunkQueueOverhead(chunkQueue, threadCount);
            System.out.printf("Threads: %2d, Avg scheduling time: %d ns%n", threadCount, overhead);
        }
    }
    
    /// Measures the overhead of ChunkQueue's synchronized scheduling methods.
    private long measureChunkQueueOverhead(ChunkQueue chunkQueue, int threadCount) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicLong totalTime = new AtomicLong(0);
        AtomicInteger completedOperations = new AtomicInteger(0);
        int operationsPerThread = 1000;
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        
        // Create concurrent scheduling tasks
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    
                    for (int i = 0; i < operationsPerThread; i++) {
                        long startTime = System.nanoTime();
                        
                        // Simulate scheduling operation
                        chunkQueue.executeScheduling(wrapper -> {
                            // Simulate scheduler adding some tasks
                            for (int j = 0; j < 5; j++) {
                                MockNodeDownloadTask task = new MockNodeDownloadTask(j, j * 1024, 1024);
                                wrapper.offerTask(task);
                            }
                        });
                        
                        long endTime = System.nanoTime();
                        totalTime.addAndGet(endTime - startTime);
                        completedOperations.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for completion
        endLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        
        return totalTime.get() / completedOperations.get();
    }
    
    /// Tests the impact of different locking strategies during concurrent reads.
    @Test
    void testReadLockContentionScenarios() throws Exception {
        System.out.println("\n=== Read Lock Contention Test ===");
        
        // Simulate different read patterns
        testReadPattern("Sequential reads (same region)", this::sequentialReadsInSameRegion);
        testReadPattern("Random reads (different regions)", this::randomReadsInDifferentRegions);
        testReadPattern("Overlapping reads", this::overlappingReads);
    }
    
    private void testReadPattern(String description, ReadPatternTest test) throws Exception {
        System.out.println("\n" + description + ":");
        
        int[] threadCounts = {1, 2, 4, 8, 16};
        
        for (int threadCount : threadCounts) {
            Duration duration = test.execute(threadCount);
            double throughput = (1000.0 * threadCount) / duration.toMillis();
            System.out.printf("  Threads: %2d, Duration: %6d ms, Throughput: %8.2f ops/sec%n", 
                            threadCount, duration.toMillis(), throughput);
        }
    }
    
    private Duration sequentialReadsInSameRegion(int threadCount) throws Exception {
        return executeReadTest(threadCount, (threadId, iteration) -> new ReadRequest(1024, 1024));
    }
    
    private Duration randomReadsInDifferentRegions(int threadCount) throws Exception {
        return executeReadTest(threadCount, (threadId, iteration) -> {
            long offset = threadId * 10000 + iteration * 1024;
            return new ReadRequest(offset, 1024);
        });
    }
    
    private Duration overlappingReads(int threadCount) throws Exception {
        return executeReadTest(threadCount, (threadId, iteration) -> {
            long offset = iteration * 512; // 50% overlap
            return new ReadRequest(offset, 1024);
        });
    }
    
    private Duration executeReadTest(int threadCount, ReadRequestGenerator generator) throws Exception {
        MockMAFileChannelTest mockChannel = new MockMAFileChannelTest();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);
        
        int operationsPerThread = 1000;
        
        Instant testStart = Instant.now();
        
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    
                    for (int i = 0; i < operationsPerThread; i++) {
                        ReadRequest request = generator.generate(threadId, i);
                        mockChannel.simulateRead(request.offset, request.length);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        startLatch.countDown(); // Start all threads
        endLatch.await(60, TimeUnit.SECONDS); // Wait for completion
        
        Instant testEnd = Instant.now();
        executor.shutdown();
        
        if (errors.get() > 0) {
            throw new RuntimeException("Test completed with " + errors.get() + " errors");
        }
        
        return Duration.between(testStart, testEnd);
    }
    
    /// Mock implementation to simulate MAFileChannel read behavior for performance testing.
    private static class MockMAFileChannelTest {
        private final Object readLock = new Object();
        
        void simulateRead(long offset, int length) throws InterruptedException {
            // Simulate the read lock acquisition and scheduling overhead
            synchronized (readLock) {
                // Simulate scheduling time
                Thread.sleep(1); // 1ms to simulate scheduling overhead
                
                // Simulate waiting for downloads
                Thread.sleep(5); // 5ms to simulate download wait
            }
        }
    }
    
    /// Tests the performance difference between selective blocking and all-blocking approaches.
    @Test 
    void testSelectiveBlockingPerformance() throws Exception {
        System.out.println("\n=== Selective Blocking vs All-Blocking Performance ===");
        
        // Create mock scenarios
        int totalTasks = 100;
        int relevantTasks = 10;
        
        // Test all-blocking approach
        long allBlockingTime = measureBlockingApproach(totalTasks, totalTasks);
        
        // Test selective blocking approach  
        long selectiveBlockingTime = measureBlockingApproach(totalTasks, relevantTasks);
        
        double improvement = (double) allBlockingTime / selectiveBlockingTime;
        
        System.out.printf("All-blocking time: %d ms%n", allBlockingTime);
        System.out.printf("Selective blocking time: %d ms%n", selectiveBlockingTime);
        System.out.printf("Performance improvement: %.2fx%n", improvement);
    }
    
    private long measureBlockingApproach(int totalTasks, int tasksToWaitFor) throws Exception {
        List<CompletableFuture<Void>> allFutures = new ArrayList<>();
        
        // Create mock futures
        for (int i = 0; i < totalTasks; i++) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            allFutures.add(future);
            
            // Complete futures with different delays to simulate real scenarios
            final int taskIndex = i;
            CompletableFuture.delayedExecutor(10 + (taskIndex % 50), TimeUnit.MILLISECONDS)
                .execute(() -> future.complete(null));
        }
        
        Instant start = Instant.now();
        
        // Wait for subset of futures (simulating selective blocking)
        List<CompletableFuture<Void>> futuresToWaitFor = allFutures.subList(0, tasksToWaitFor);
        CompletableFuture.allOf(futuresToWaitFor.toArray(new CompletableFuture[0])).join();
        
        Instant end = Instant.now();
        
        return Duration.between(start, end).toMillis();
    }
    
    /// Helper classes and interfaces for the performance tests.
    
    @FunctionalInterface
    private interface ReadPatternTest {
        Duration execute(int threadCount) throws Exception;
    }
    
    @FunctionalInterface
    private interface ReadRequestGenerator {
        ReadRequest generate(int threadId, int iteration);
    }
    
    private static class ReadRequest {
        final long offset;
        final int length;
        
        ReadRequest(long offset, int length) {
            this.offset = offset;
            this.length = length;
        }
    }
    
    /// Mock task implementation for testing ChunkQueue overhead.
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
            // Mock implementation - return a simple range for the leaf node
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