package io.nosqlbench.vectordata.transport;

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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

/// File-specific implementation of StreamingFetchResult.
/// 
/// This class wraps a FileChannel and provides streaming access to a specific
/// range of the file. It manages the lifecycle of the file channel, ensuring
/// proper cleanup when the result is closed.
/// 
/// The implementation uses a limited channel wrapper to ensure that only the
/// requested range of bytes can be read, preventing accidental over-reading.
public class FileStreamingFetchResult implements StreamingFetchResult {
    
    private final FileChannel fileChannel;
    private final ReadableByteChannel limitedChannel;
    private final long offset;
    private final long requestedLength;
    private final long actualLength;
    private final String source;
    private volatile boolean closed = false;
    
    /// Creates a new FileStreamingFetchResult.
    /// 
    /// @param fileChannel The underlying file channel (ownership is transferred)
    /// @param limitedChannel The limited channel that restricts reading to the range
    /// @param offset The offset from which the data is being read
    /// @param requestedLength The number of bytes that were requested
    /// @param actualLength The actual number of bytes available (may be less at EOF)
    /// @param source The source file path as a URI string
    public FileStreamingFetchResult(FileChannel fileChannel, ReadableByteChannel limitedChannel,
                                  long offset, long requestedLength, long actualLength, String source) {
        this.fileChannel = fileChannel;
        this.limitedChannel = limitedChannel;
        this.offset = offset;
        this.requestedLength = requestedLength;
        this.actualLength = actualLength;
        this.source = source;
    }
    
    @Override
    public ReadableByteChannel getDataChannel() {
        if (closed) {
            throw new IllegalStateException("StreamingFetchResult has been closed");
        }
        return limitedChannel;
    }
    
    @Override
    public long getOffset() {
        return offset;
    }
    
    @Override
    public long getRequestedLength() {
        return requestedLength;
    }
    
    @Override
    public long getActualLength() {
        return actualLength;
    }
    
    @Override
    public boolean isSuccessful() {
        return !closed && fileChannel.isOpen();
    }
    
    @Override
    public String getSource() {
        return source;
    }
    
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            try {
                // The limited channel doesn't need separate closing
                // as it delegates to the file channel
            } finally {
                fileChannel.close();
            }
        }
    }
}