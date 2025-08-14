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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/// Performance test comparing global locking vs region-based locking.
/// 
/// This test simulates the read patterns that would occur in MAFileChannel
/// and measures the performance difference between using a single global
/// read/write lock vs fine-grained region-based locking.
@Tag("performance")
public class RegionBasedLockingTest {
    
    private static final int OPERATIONS_PER_THREAD = 5000;
    private static final int READ_SIZE = 4096; // 4KB reads
    private static final long FILE_SIZE = 100 * 1024 * 1024; // 100MB file
    
    /// Compares global locking vs region-based locking performance.
    @Test
    void compareGlobalVsRegionLocking() throws Exception {
        System.out.println("\n=== Global vs Region-Based Locking Performance ===");
        System.out.println("File size: " + FILE_SIZE / (1024 * 1024) + " MB");
        System.out.println("Read size: " + READ_SIZE + " bytes");
        System.out.println("Operations per thread: " + OPERATIONS_PER_THREAD);
        System.out.println();
        
        int[] threadCounts = {1, 2, 4, 8, 16, 32};
        
        for (int threadCount : threadCounts) {
            System.out.printf("Threads: %2d\n", threadCount);
            
            // Test global locking
            Duration globalTime = testGlobalLocking(threadCount);
            
            // Test region-based locking
            Duration regionTime = testRegionBasedLocking(threadCount);
            
            double improvement = (double) globalTime.toNanos() / regionTime.toNanos();
            
            System.out.printf("  Global:  %6d ms\n", globalTime.toMillis());
            System.out.printf("  Region:  %6d ms\n", regionTime.toMillis());
            System.out.printf("  Improvement: %.2fx\n", improvement);
            System.out.println();
        }
    }
    
    /// Tests performance with global read/write lock.
    private Duration testGlobalLocking(int threadCount) throws Exception {
        ReadWriteLock globalLock = new ReentrantReadWriteLock();
        
        return executeReadTest(threadCount, (offset, length) -> {
            // Simulate read operation with global lock
            globalLock.readLock().lock();
            try {
                // Simulate some read work
                Thread.sleep(1); // 1ms to simulate I/O
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                globalLock.readLock().unlock();
            }
        });
    }
    
    /// Tests performance with region-based locking.
    private Duration testRegionBasedLocking(int threadCount) throws Exception {
        RegionBasedLocking regionLocking = new RegionBasedLocking();
        
        return executeReadTest(threadCount, (offset, length) -> {
            // Simulate read operation with region-based lock
            RegionBasedLocking.RegionLockHandle lock = regionLocking.getReadLock(offset, length);
            lock.lock();
            try {
                // Simulate some read work
                Thread.sleep(1); // 1ms to simulate I/O
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        });
    }
    
    /// Executes a read test with the specified locking strategy.
    private Duration executeReadTest(int threadCount, ReadOperation readOperation) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);
        
        Instant startTime = Instant.now();
        
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    
                    // Each thread reads from different parts of the file to maximize parallelism potential
                    long baseOffset = (FILE_SIZE / threadCount) * threadId;
                    
                    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
                        long offset = baseOffset + (i * READ_SIZE) % (FILE_SIZE / threadCount);
                        readOperation.execute(offset, READ_SIZE);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        startLatch.countDown(); // Start all threads
        endLatch.await(120, TimeUnit.SECONDS); // Wait for completion
        
        Instant endTime = Instant.now();
        executor.shutdown();
        
        if (errors.get() > 0) {
            throw new RuntimeException("Test failed with " + errors.get() + " errors");
        }
        
        return Duration.between(startTime, endTime);
    }
    
    /// Tests the correctness of region-based locking.
    @Test
    void testRegionBasedLockingCorrectness() throws Exception {
        System.out.println("\n=== Region-Based Locking Correctness Test ===");
        
        RegionBasedLocking regionLocking = new RegionBasedLocking();
        int threadCount = 16;
        int operationsPerThread = 1000;
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger successfulOps = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);
        
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    
                    for (int i = 0; i < operationsPerThread; i++) {
                        long offset = threadId * 100000L + i * 4096L;
                        int length = 4096;
                        
                        // Test read lock
                        RegionBasedLocking.RegionLockHandle readLock = regionLocking.getReadLock(offset, length);
                        readLock.lock();
                        try {
                            // Simulate read work
                            Thread.sleep(1);
                            successfulOps.incrementAndGet();
                        } finally {
                            readLock.unlock();
                        }
                        
                        // Test write lock (less frequently)
                        if (i % 10 == 0) {
                            RegionBasedLocking.RegionLockHandle writeLock = regionLocking.getWriteLock(offset, length);
                            writeLock.lock();
                            try {
                                // Simulate write work
                                Thread.sleep(1);
                                successfulOps.incrementAndGet();
                            } finally {
                                writeLock.unlock();
                            }
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
        endLatch.await(60, TimeUnit.SECONDS);
        executor.shutdown();
        
        int expectedReadOps = threadCount * operationsPerThread;
        int expectedWriteOps = threadCount * (operationsPerThread / 10);
        int expectedTotalOps = expectedReadOps + expectedWriteOps;
        
        System.out.printf("Expected operations: %d\n", expectedTotalOps);
        System.out.printf("Successful operations: %d\n", successfulOps.get());
        System.out.printf("Errors: %d\n", errors.get());
        
        RegionBasedLocking.LockingStats stats = regionLocking.getStats();
        System.out.printf("Active regions: %d\n", stats.getActiveRegions());
        System.out.printf("Region size: %d bytes\n", stats.getRegionSize());
        
        if (errors.get() > 0) {
            throw new AssertionError("Correctness test failed with " + errors.get() + " errors");
        }
        
        System.out.println("✓ Region-based locking correctness test passed");
    }
    
    /// Tests different access patterns to show region-based locking benefits.
    @Test
    void testDifferentAccessPatterns() throws Exception {
        System.out.println("\n=== Access Pattern Performance Analysis ===");
        
        int threadCount = 8;
        
        // Test 1: Sequential access (same region) - should show minimal improvement
        System.out.println("Sequential access (same region):");
        Duration globalSeq = testAccessPattern(threadCount, true, false);
        Duration regionSeq = testAccessPattern(threadCount, false, false);
        System.out.printf("  Global: %d ms, Region: %d ms, Improvement: %.2fx\n", 
                         globalSeq.toMillis(), regionSeq.toMillis(), 
                         (double) globalSeq.toNanos() / regionSeq.toNanos());
        
        // Test 2: Random access (different regions) - should show significant improvement
        System.out.println("Random access (different regions):");
        Duration globalRand = testAccessPattern(threadCount, true, true);
        Duration regionRand = testAccessPattern(threadCount, false, true);
        System.out.printf("  Global: %d ms, Region: %d ms, Improvement: %.2fx\n", 
                         globalRand.toMillis(), regionRand.toMillis(), 
                         (double) globalRand.toNanos() / regionRand.toNanos());
    }
    
    /// Tests a specific access pattern with global or region-based locking.
    private Duration testAccessPattern(int threadCount, boolean useGlobalLock, boolean randomAccess) throws Exception {
        ReadWriteLock globalLock = useGlobalLock ? new ReentrantReadWriteLock() : null;
        RegionBasedLocking regionLocking = useGlobalLock ? null : new RegionBasedLocking();
        
        return executeReadTest(threadCount, (offset, length) -> {
            // Adjust offset based on access pattern
            long actualOffset = randomAccess ? 
                (offset + System.nanoTime()) % FILE_SIZE :  // Random access
                offset % (10 * 1024 * 1024);               // Sequential in small region
            
            if (useGlobalLock) {
                globalLock.readLock().lock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    globalLock.readLock().unlock();
                }
            } else {
                RegionBasedLocking.RegionLockHandle lock = regionLocking.getReadLock(actualOffset, length);
                lock.lock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } finally {
                    lock.unlock();
                }
            }
        });
    }
    
    /// Functional interface for read operations.
    @FunctionalInterface
    private interface ReadOperation {
        void execute(long offset, int length) throws Exception;
    }
}