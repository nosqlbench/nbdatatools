package io.nosqlbench.nbdatatools.api.transport;

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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.io.Closeable;

/// Result wrapper for streaming fetch operations that supports large data transfers.
/// 
/// This interface extends the concept of FetchResult to support streaming data
/// through a ReadableByteChannel, which allows for efficient memory usage when
/// dealing with large data transfers that exceed 2GB or when streaming is preferred
/// over loading entire chunks into memory.
/// 
/// The ReadableByteChannel allows callers to read data incrementally, making it
/// suitable for:
/// - Large file transfers exceeding ByteBuffer capacity
/// - Streaming data processing without full buffering
/// - Memory-efficient data handling
/// - Progressive data consumption
/// 
/// Example usage:
/// ```java
/// StreamingFetchResult result = client.fetchRangeStreaming(0, 10_000_000_000L).get();
/// try (ReadableByteChannel channel = result.getDataChannel()) {
///     ByteBuffer buffer = ByteBuffer.allocate(8192);
///     while (channel.read(buffer) != -1) {
///         buffer.flip();
///         // Process buffer data
///         buffer.clear();
///     }
/// }
/// ```
public interface StreamingFetchResult extends Closeable {
    
    /// Gets the data channel for reading the fetched content.
    /// 
    /// The returned channel should be closed by the caller when done to ensure
    /// proper resource cleanup. The channel supports standard NIO operations
    /// and can be used with ByteBuffers of any size.
    /// 
    /// @return A ReadableByteChannel for accessing the fetched data
    ReadableByteChannel getDataChannel();
    
    /// Gets the offset from which the data was fetched.
    /// 
    /// @return The offset in bytes
    long getOffset();
    
    /// Gets the number of bytes that were requested.
    /// 
    /// Note: This uses long to support requests larger than 2GB.
    /// 
    /// @return The requested length in bytes
    long getRequestedLength();
    
    /// Gets the actual number of bytes available for reading.
    /// 
    /// This may be less than the requested length if the end of the resource
    /// was reached. For streaming results, this might return -1 if the total
    /// size is not known in advance.
    /// 
    /// @return The actual number of bytes available, or -1 if unknown
    long getActualLength();
    
    /// Checks if the fetch was successful.
    /// 
    /// A fetch is considered successful if a data channel is available for reading.
    /// 
    /// @return true if data can be read from the channel, false otherwise
    boolean isSuccessful();
    
    /// Gets the source identifier for this fetch result.
    /// 
    /// @return The source URL or path
    String getSource();
    
    /// Closes the underlying data channel and releases resources.
    /// 
    /// This method should be called when the data has been consumed to ensure
    /// proper cleanup. After calling close(), the data channel should not be used.
    /// 
    /// @throws IOException if an error occurs during cleanup
    @Override
    void close() throws IOException;
}