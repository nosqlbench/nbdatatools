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

import io.nosqlbench.nbdatatools.api.transport.StreamingFetchResult;
import io.nosqlbench.vectordata.events.EventSink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/// A wrapper for StreamingFetchResult that tracks progress as data is read.
/// 
/// This class decorates a StreamingFetchResult with progress tracking capabilities.
/// It wraps the underlying ReadableByteChannel to monitor bytes as they are read,
/// updating cumulative statistics and reporting progress through the EventSink.
public class ProgressTrackingStreamingResult implements StreamingFetchResult {
    
    private final StreamingFetchResult delegate;
    private final Instant startTime;
    private final int chunkIndex;
    private final int totalChunks;
    private final AtomicLong cumulativeBytesTransferred;
    private final EventSink eventSink;
    private final ProgressTrackingChannel trackingChannel;
    private final AtomicLong localBytesRead = new AtomicLong(0);
    
    public ProgressTrackingStreamingResult(StreamingFetchResult delegate, 
                                         Instant startTime,
                                         int chunkIndex,
                                         int totalChunks,
                                         AtomicLong cumulativeBytesTransferred,
                                         EventSink eventSink) {
        this.delegate = delegate;
        this.startTime = startTime;
        this.chunkIndex = chunkIndex;
        this.totalChunks = totalChunks;
        this.cumulativeBytesTransferred = cumulativeBytesTransferred;
        this.eventSink = eventSink;
        this.trackingChannel = new ProgressTrackingChannel(delegate.getDataChannel());
    }
    
    @Override
    public ReadableByteChannel getDataChannel() {
        return trackingChannel;
    }
    
    @Override
    public long getOffset() {
        return delegate.getOffset();
    }
    
    @Override
    public long getRequestedLength() {
        return delegate.getRequestedLength();
    }
    
    @Override
    public long getActualLength() {
        return delegate.getActualLength();
    }
    
    @Override
    public boolean isSuccessful() {
        return delegate.isSuccessful();
    }
    
    @Override
    public String getSource() {
        return delegate.getSource();
    }
    
    @Override
    public void close() throws IOException {
        try {
            // Report final progress
            long totalRead = localBytesRead.get();
            if (totalRead > 0) {
                Duration duration = Duration.between(startTime, Instant.now());
                double throughputMBps = duration.toMillis() > 0 ? 
                    (totalRead / 1024.0 / 1024.0) / (duration.toMillis() / 1000.0) : 0;
                
                eventSink.info("Completed streaming chunk {}/{}: {} bytes in {}ms ({:.2f} MB/s)",
                    chunkIndex + 1, totalChunks, totalRead, 
                    duration.toMillis(), throughputMBps);
            }
        } finally {
            delegate.close();
        }
    }
    
    /// Inner class that wraps the ReadableByteChannel to track bytes read
    private class ProgressTrackingChannel implements ReadableByteChannel {
        private final ReadableByteChannel delegate;
        private long lastProgressReport = 0;
        private static final long PROGRESS_REPORT_INTERVAL = 10 * 1024 * 1024; // Report every 10MB
        
        ProgressTrackingChannel(ReadableByteChannel delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public int read(ByteBuffer dst) throws IOException {
            int bytesRead = delegate.read(dst);
            
            if (bytesRead > 0) {
                long newLocal = localBytesRead.addAndGet(bytesRead);
                long newCumulative = cumulativeBytesTransferred.addAndGet(bytesRead);
                
                // Report progress periodically (every 10MB)
                if (newLocal - lastProgressReport >= PROGRESS_REPORT_INTERVAL) {
                    Duration elapsed = Duration.between(startTime, Instant.now());
                    double throughputMBps = elapsed.toMillis() > 0 ? 
                        (newLocal / 1024.0 / 1024.0) / (elapsed.toMillis() / 1000.0) : 0;
                    
                    eventSink.debug("Streaming chunk {}/{}: {} MB read ({:.2f} MB/s)",
                        chunkIndex + 1, totalChunks, 
                        newLocal / (1024 * 1024),
                        throughputMBps);
                    
                    lastProgressReport = newLocal;
                }
            }
            
            return bytesRead;
        }
        
        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }
        
        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}