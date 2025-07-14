package io.nosqlbench.vectordata.downloader;

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
import io.nosqlbench.vectordata.status.EventSink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/// Decorator for ChunkedTransportClient that adds progress tracking capabilities.
/// 
/// This wrapper enhances a standard ChunkedTransportClient by returning
/// ProgressTrackingFetchResult instances that include detailed timing and
/// progress information. It maintains cumulative statistics across multiple
/// fetch operations and integrates with the EventSink for progress reporting.
/// 
/// Example usage:
/// ```java
/// ChunkedTransportClient baseClient = ChunkedTransportIO.create(url);
/// ProgressTrackingTransportClient trackingClient = new ProgressTrackingTransportClient(
///     baseClient, totalSize, eventSink
/// );
/// ProgressTrackingFetchResult result = trackingClient.fetchRange(0, 1024).get();
/// ```
public class ProgressTrackingTransportClient implements ChunkedTransportClient {
    
    private final ChunkedTransportClient delegate;
    private final long totalResourceSize;
    private final AtomicLong cumulativeBytesTransferred;
    private final EventSink eventSink;
    private final AtomicLong chunkCounter;
    private final int totalChunks;
    
    /// Creates a new ProgressTrackingTransportClient.
    /// 
    /// @param delegate The underlying transport client to wrap
    /// @param totalResourceSize Total size of the resource being downloaded
    /// @param totalChunks Total number of chunks that will be downloaded
    /// @param eventSink Event sink for progress notifications
    public ProgressTrackingTransportClient(ChunkedTransportClient delegate, 
                                         long totalResourceSize,
                                         int totalChunks,
                                         EventSink eventSink) {
        this.delegate = delegate;
        this.totalResourceSize = totalResourceSize;
        this.totalChunks = totalChunks;
        this.eventSink = eventSink;
        this.cumulativeBytesTransferred = new AtomicLong(0);
        this.chunkCounter = new AtomicLong(0);
    }
    
    @Override
    public CompletableFuture<ProgressTrackingFetchResult> fetchRange(long offset, int length) throws IOException {
        Instant startTime = Instant.now();
        int chunkIndex = (int) chunkCounter.getAndIncrement();
        
        // Use the new FetchResult-based method
        return delegate.fetchRange(offset, length)
            .thenApply(result -> {
                Instant endTime = Instant.now();
                ByteBuffer buffer = result.getData();
                long transferred = buffer.remaining();
                long cumulative = cumulativeBytesTransferred.addAndGet(transferred);
                
                ProgressTrackingFetchResult trackingResult = ProgressTrackingFetchResult.builder()
                    .data(buffer)
                    .offset(offset)
                    .requestedLength(length)
                    .startTime(startTime)
                    .endTime(endTime)
                    .totalResourceSize(totalResourceSize)
                    .cumulativeBytesTransferred(cumulative)
                    .source(getSource())
                    .chunkIndex(chunkIndex)
                    .totalChunks(totalChunks)
                    .build();
                
                // Report progress through event sink
                eventSink.debug("Downloaded chunk {}/{}: {} bytes ({}% complete, {:.2f} MB/s)",
                    chunkIndex + 1, totalChunks, transferred,
                    String.format("%.1f", trackingResult.getProgressPercentage()),
                    trackingResult.getThroughputBytesPerSecond() / (1024 * 1024));
                
                return trackingResult;
            });
    }
    
    @Override
    @Deprecated
    public CompletableFuture<ByteBuffer> fetchRangeRaw(long offset, int length) throws IOException {
        // This shouldn't be called directly when using progress tracking
        return fetchRange(offset, length).thenApply(FetchResult::getData);
    }
    
    @Override
    public CompletableFuture<Long> getSize() throws IOException {
        return delegate.getSize();
    }
    
    @Override
    public boolean supportsRangeRequests() {
        return delegate.supportsRangeRequests();
    }
    
    @Override
    public String getSource() {
        return delegate.getSource();
    }
    
    @Override
    public void close() throws IOException {
        delegate.close();
    }
    
    /// Gets the cumulative bytes transferred so far.
    /// 
    /// @return Total bytes transferred across all fetch operations
    public long getCumulativeBytesTransferred() {
        return cumulativeBytesTransferred.get();
    }
    
    /// Gets the overall download progress as a percentage.
    /// 
    /// @return Progress percentage (0.0 to 100.0)
    public double getOverallProgressPercentage() {
        if (totalResourceSize <= 0) {
            return 0.0;
        }
        return (cumulativeBytesTransferred.get() * 100.0) / totalResourceSize;
    }
}