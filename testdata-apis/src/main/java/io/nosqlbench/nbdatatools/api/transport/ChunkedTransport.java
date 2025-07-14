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
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/// Contract for fetching byte ranges from various sources (URLs, files, etc.).
/// 
/// This interface provides asynchronous access to byte ranges from different data sources,
/// with implementations supporting both HTTP URLs and local files. All operations are
/// designed to be thread-safe and support arbitrarily large files through efficient
/// range-based access patterns.
/// 
/// Implementations should handle:
/// - Concurrent access from multiple threads
/// - Efficient partial reads without loading entire files
/// - Proper resource management and cleanup
/// - Error handling for network and I/O failures
/// 
/// Example usage:
/// ```java
/// ByteRangeFetcher fetcher = ByteRangeFetcherFactory.create("https://example.com/data.bin");
/// CompletableFuture<ByteBuffer> future = fetcher.fetchRange(1024, 2048);
/// ByteBuffer data = future.get(); // 1024 bytes starting at offset 1024
/// ```
public interface ChunkedTransport extends AutoCloseable {

    /// Fetches a range of bytes from the data source asynchronously.
    /// 
    /// @param offset The starting byte offset (0-based)
    /// @param length The number of bytes to fetch
    /// @return A CompletableFuture containing the requested byte range as a ByteBuffer
    /// @throws IllegalArgumentException if offset is negative or length is non-positive
    /// @throws IOException if the source cannot be accessed or the range is invalid
    CompletableFuture<ByteBuffer> fetchRange(long offset, int length) throws IOException;

    /// Gets the total size of the data source in bytes.
    /// 
    /// This method may perform a network request for HTTP sources to determine the content length,
    /// or read file metadata for local files. The result may be cached for subsequent calls.
    /// 
    /// @return A CompletableFuture containing the total size in bytes
    /// @throws IOException if the size cannot be determined
    CompletableFuture<Long> getSize() throws IOException;

    /// Checks if this fetcher supports range requests.
    /// 
    /// For HTTP sources, this indicates whether the server supports HTTP Range requests.
    /// For file sources, this should always return true.
    /// 
    /// @return true if range requests are supported, false otherwise
    boolean supportsRangeRequests();

    /// Gets the source URL or path that this fetcher is reading from.
    /// 
    /// @return The source URL string (e.g., "https://example.com/file.dat" or "file:///path/to/file.dat")
    String getSource();

    /// Closes the fetcher and releases any associated resources.
    /// 
    /// This method should be called when the fetcher is no longer needed to ensure
    /// proper cleanup of network connections, file handles, and other resources.
    /// After calling close(), the behavior of other methods is undefined.
    /// 
    /// @throws IOException if an error occurs during cleanup
    @Override
    void close() throws IOException;
}