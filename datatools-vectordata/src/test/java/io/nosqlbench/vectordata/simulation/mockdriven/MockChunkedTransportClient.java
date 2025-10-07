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

import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient;
import io.nosqlbench.nbdatatools.api.transport.FetchResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/// Mock implementation of ChunkedTransportClient for simulation testing.
/// 
/// This mock transport client simulates realistic network behavior including:
/// - Configurable bandwidth limitations
/// - Network latency simulation
/// - Connection concurrency limits
/// - Reliability simulation (success/failure rates)
/// - Progress tracking and statistics
/// 
/// The client can be configured to simulate various network conditions to test
/// how different scheduling strategies perform under different constraints.
/// 
/// Example usage:
/// ```java
/// NetworkConditions conditions = new NetworkConditions(
///     50_000_000, // 50 Mbps bandwidth
///     Duration.ofMillis(100), // 100ms latency
///     4, // max concurrent connections
///     0.99 // 99% success rate
/// );
/// 
/// MockChunkedTransportClient client = new MockChunkedTransportClient(
///     1_000_000_000L, // 1GB file size
///     conditions
/// );
/// ```
public class MockChunkedTransportClient implements ChunkedTransportClient {
    
    private final long fileSize;
    private final NetworkConditions networkConditions;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong totalBytesTransferred = new AtomicLong(0);
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong failedRequests = new AtomicLong(0);
    
    /// Connection pool to simulate bandwidth and concurrency limits
    private final Semaphore connectionSemaphore;
    private final ScheduledExecutorService bandwidthScheduler;
    private final ExecutorService requestExecutor;
    
    /// Statistics tracking
    private final Instant startTime = Instant.now();
    private volatile Instant lastRequestTime = startTime;
    
    /// Creates a mock transport client with specified network conditions.
    /// 
    /// @param fileSize The total size of the simulated file
    /// @param networkConditions The network conditions to simulate
    public MockChunkedTransportClient(long fileSize, NetworkConditions networkConditions) {
        if (fileSize <= 0) {
            throw new IllegalArgumentException("File size must be positive");
        }
        if (networkConditions == null) {
            throw new IllegalArgumentException("Network conditions cannot be null");
        }
        
        this.fileSize = fileSize;
        this.networkConditions = networkConditions;
        this.connectionSemaphore = new Semaphore(networkConditions.getMaxConcurrentConnections());
        this.bandwidthScheduler = Executors.newScheduledThreadPool(2);
        this.requestExecutor = Executors.newCachedThreadPool();
    }
    
    @Override
    public CompletableFuture<FetchResult<?>> fetchRange(long offset, long length) throws IOException {
        validateNotClosed();
        validateRange(offset, length);
        
        totalRequests.incrementAndGet();
        lastRequestTime = Instant.now();
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Simulate connection acquisition
                connectionSemaphore.acquire();
                
                try {
                    // Simulate network latency
                    simulateLatency();
                    
                    // Simulate potential failure
                    if (shouldSimulateFailure()) {
                        failedRequests.incrementAndGet();
                        throw new RuntimeException("Simulated network failure");
                    }
                    
                    // Simulate bandwidth-limited transfer
                    long transferStartTime = System.nanoTime();
                    simulateBandwidthLimitedTransfer(length);
                    long transferEndTime = System.nanoTime();
                    
                    // Update statistics
                    totalBytesTransferred.addAndGet(length);
                    
                    // Generate mock data
                    ByteBuffer data = generateMockData(offset, length);
                    
                    return new SimulatedFetchResult(
                        data, 
                        offset, 
                        length,
                        Duration.ofNanos(transferEndTime - transferStartTime)
                    );
                    
                } finally {
                    connectionSemaphore.release();
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Transfer interrupted", e);
            }
        }, requestExecutor);
    }
    
    @Override
    public CompletableFuture<Long> getSize() throws IOException {
        validateNotClosed();
        
        // Simulate a quick HEAD request with minimal latency
        return CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(networkConditions.getLatency().toMillis() / 4); // Faster than data transfer
                return fileSize;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Size request interrupted", e);
            }
        }, requestExecutor);
    }
    
    @Override
    public boolean supportsRangeRequests() {
        return true; // Mock always supports range requests
    }
    
    @Override
    public String getSource() {
        return "mock://" + networkConditions.getDescription() + "/file.bin";
    }
    
    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            bandwidthScheduler.shutdown();
            requestExecutor.shutdown();
            
            try {
                if (!bandwidthScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    bandwidthScheduler.shutdownNow();
                }
                if (!requestExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    requestExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                bandwidthScheduler.shutdownNow();
                requestExecutor.shutdownNow();
            }
        }
    }
    
    /// Gets performance statistics for this transport client.
    /// 
    /// @return Current performance statistics
    public TransportStatistics getStatistics() {
        Duration totalTime = Duration.between(startTime, lastRequestTime);
        long bytes = totalBytesTransferred.get();
        long requests = totalRequests.get();
        long failures = failedRequests.get();
        
        double throughputBps = totalTime.toNanos() > 0 ? 
            (bytes * 1_000_000_000.0) / totalTime.toNanos() : 0.0;
        
        return new TransportStatistics(
            bytes,
            requests,
            failures,
            throughputBps,
            totalTime,
            networkConditions
        );
    }
    
    /// Validates that the client has not been closed.
    private void validateNotClosed() throws IOException {
        if (closed.get()) {
            throw new IOException("MockChunkedTransportClient has been closed");
        }
    }
    
    /// Validates that the requested range is valid.
    private void validateRange(long offset, long length) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("Length must be positive: " + length);
        }
        if (offset >= fileSize) {
            throw new IllegalArgumentException("Offset beyond file size: " + offset + " >= " + fileSize);
        }
        if (offset + length > fileSize) {
            throw new IllegalArgumentException("Range extends beyond file size");
        }
    }
    
    /// Simulates network latency by sleeping.
    private void simulateLatency() throws InterruptedException {
        Duration latency = networkConditions.getLatency();
        if (latency.toMillis() > 0) {
            Thread.sleep(latency.toMillis());
        }
    }
    
    /// Determines if this request should simulate a failure.
    private boolean shouldSimulateFailure() {
        return Math.random() > networkConditions.getSuccessRate();
    }
    
    /// Simulates bandwidth-limited data transfer.
    private void simulateBandwidthLimitedTransfer(long length) throws InterruptedException {
        long bandwidthBps = networkConditions.getBandwidthBps();
        if (bandwidthBps <= 0) {
            return; // No bandwidth limit
        }
        
        // Calculate transfer time based on bandwidth
        long transferTimeMs = (length * 1000L) / bandwidthBps;
        if (transferTimeMs > 0) {
            Thread.sleep(transferTimeMs);
        }
    }
    
    /// Generates mock data for the specified range.
    private ByteBuffer generateMockData(long offset, long length) {
        ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(length, Integer.MAX_VALUE));
        
        // Generate deterministic but varied data based on offset
        for (int i = 0; i < Math.min(length, Integer.MAX_VALUE); i++) {
            long position = offset + i;
            byte value = (byte) ((position * 31 + position / 1024) % 256);
            buffer.put(value);
        }
        
        buffer.flip();
        return buffer;
    }
    
    /// Simulated fetch result that includes timing information.
    private static class SimulatedFetchResult extends FetchResult<SimulatedFetchResult> {
        private final Duration transferTime;
        
        public SimulatedFetchResult(ByteBuffer data, long offset, long requestedLength, Duration transferTime) {
            super(data, offset, requestedLength);
            this.transferTime = transferTime;
        }
        
        @Override
        public ByteBuffer getData() {
            return super.getData().duplicate(); // Return a duplicate to avoid state changes
        }
        
        public Duration getTransferTime() {
            return transferTime;
        }
    }
}