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

import io.nosqlbench.nbdatatools.api.transport.FetchResult;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.Duration;

/// Extended FetchResult that includes progress tracking and status information.
/// 
/// This decorator extends the base FetchResult to provide additional metadata
/// about the fetch operation, including timing information, transfer rates,
/// and progress status. This allows the ChunkedResourceTransportService to
/// provide detailed progress information while maintaining compatibility with
/// the base ChunkedTransportClient interface.
/// 
/// Example usage:
/// ```java
/// ProgressTrackingFetchResult result = service.fetchRange(0, 1024).get();
/// ByteBuffer data = result.getData();
/// long transferTime = result.getTransferDuration().toMillis();
/// double throughput = result.getThroughputBytesPerSecond();
/// ```
public class ProgressTrackingFetchResult extends FetchResult<ProgressTrackingFetchResult> {
    
    private final Instant startTime;
    private final Instant endTime;
    private final long totalResourceSize;
    private final long cumulativeBytesTransferred;
    private final String source;
    private final int chunkIndex;
    private final int totalChunks;
    
    /// Creates a new ProgressTrackingFetchResult with full progress information.
    /// 
    /// @param data The fetched data
    /// @param offset The offset from which data was fetched
    /// @param requestedLength The number of bytes requested
    /// @param startTime When the fetch operation started
    /// @param endTime When the fetch operation completed
    /// @param totalResourceSize Total size of the resource being downloaded
    /// @param cumulativeBytesTransferred Total bytes transferred so far across all chunks
    /// @param source The source URL or path
    /// @param chunkIndex The index of this chunk (0-based)
    /// @param totalChunks Total number of chunks in the download
    public ProgressTrackingFetchResult(ByteBuffer data, long offset, int requestedLength,
                                     Instant startTime, Instant endTime,
                                     long totalResourceSize, long cumulativeBytesTransferred,
                                     String source, int chunkIndex, int totalChunks) {
        super(data, offset, requestedLength);
        this.startTime = startTime;
        this.endTime = endTime;
        this.totalResourceSize = totalResourceSize;
        this.cumulativeBytesTransferred = cumulativeBytesTransferred;
        this.source = source;
        this.chunkIndex = chunkIndex;
        this.totalChunks = totalChunks;
    }
    
    /// Gets the time when this fetch operation started.
    /// 
    /// @return The start time
    public Instant getStartTime() {
        return startTime;
    }
    
    /// Gets the time when this fetch operation completed.
    /// 
    /// @return The end time
    public Instant getEndTime() {
        return endTime;
    }
    
    /// Gets the duration of the transfer operation.
    /// 
    /// @return The duration between start and end time
    public Duration getTransferDuration() {
        return Duration.between(startTime, endTime);
    }
    
    /// Gets the total size of the resource being downloaded.
    /// 
    /// @return The total resource size in bytes
    public long getTotalResourceSize() {
        return totalResourceSize;
    }
    
    /// Gets the cumulative number of bytes transferred so far.
    /// 
    /// This includes bytes from all previous chunks plus this chunk.
    /// 
    /// @return The cumulative bytes transferred
    public long getCumulativeBytesTransferred() {
        return cumulativeBytesTransferred;
    }
    
    /// Gets the overall progress as a percentage.
    /// 
    /// @return Progress percentage (0.0 to 100.0)
    public double getProgressPercentage() {
        if (totalResourceSize <= 0) {
            return 0.0;
        }
        return (cumulativeBytesTransferred * 100.0) / totalResourceSize;
    }
    
    /// Gets the throughput of this specific chunk transfer.
    /// 
    /// @return Throughput in bytes per second
    public double getThroughputBytesPerSecond() {
        Duration duration = getTransferDuration();
        if (duration.isZero() || duration.isNegative()) {
            return 0.0;
        }
        double seconds = duration.toNanos() / 1_000_000_000.0;
        return getActualLength() / seconds;
    }
    
    /// Gets the source URL or path.
    /// 
    /// @return The source string
    public String getSource() {
        return source;
    }
    
    /// Gets the index of this chunk.
    /// 
    /// @return The chunk index (0-based)
    public int getChunkIndex() {
        return chunkIndex;
    }
    
    /// Gets the total number of chunks in the download.
    /// 
    /// @return The total number of chunks
    public int getTotalChunks() {
        return totalChunks;
    }
    
    /// Checks if this is the final chunk.
    /// 
    /// @return true if this is the last chunk
    public boolean isFinalChunk() {
        return chunkIndex == totalChunks - 1;
    }
    
    /// Creates a builder for constructing ProgressTrackingFetchResult instances.
    /// 
    /// @return A new builder instance
    public static Builder builder() {
        return new Builder();
    }
    
    /// Builder for creating ProgressTrackingFetchResult instances.
    public static class Builder {
        private ByteBuffer data;
        private long offset;
        private int requestedLength;
        private Instant startTime;
        private Instant endTime;
        private long totalResourceSize;
        private long cumulativeBytesTransferred;
        private String source;
        private int chunkIndex;
        private int totalChunks;
        
        public Builder data(ByteBuffer data) {
            this.data = data;
            return this;
        }
        
        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }
        
        public Builder requestedLength(int requestedLength) {
            this.requestedLength = requestedLength;
            return this;
        }
        
        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }
        
        public Builder endTime(Instant endTime) {
            this.endTime = endTime;
            return this;
        }
        
        public Builder totalResourceSize(long totalResourceSize) {
            this.totalResourceSize = totalResourceSize;
            return this;
        }
        
        public Builder cumulativeBytesTransferred(long cumulativeBytesTransferred) {
            this.cumulativeBytesTransferred = cumulativeBytesTransferred;
            return this;
        }
        
        public Builder source(String source) {
            this.source = source;
            return this;
        }
        
        public Builder chunkIndex(int chunkIndex) {
            this.chunkIndex = chunkIndex;
            return this;
        }
        
        public Builder totalChunks(int totalChunks) {
            this.totalChunks = totalChunks;
            return this;
        }
        
        public ProgressTrackingFetchResult build() {
            return new ProgressTrackingFetchResult(
                data, offset, requestedLength,
                startTime, endTime,
                totalResourceSize, cumulativeBytesTransferred,
                source, chunkIndex, totalChunks
            );
        }
    }
}