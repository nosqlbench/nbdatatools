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

import io.nosqlbench.nbdatatools.api.services.TransportScheme;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/// File-based implementation of ByteRangeFetcher using AsynchronousFileChannel and memory mapping.
/// 
/// This implementation provides efficient range-based access to local files using:
/// - AsynchronousFileChannel for thread-safe concurrent access
/// - Memory-mapped files for efficient partial reads of single requests
/// - Caching of file size and channel for optimal performance
/// 
/// Key features:
/// - Thread-safe concurrent access using absolute positioning
/// - Memory mapping for efficient partial reads without loading entire file
/// - Support for arbitrarily large files through range-based access
/// - Proper resource cleanup and file handle management
/// - Validation of file existence and readability
/// 
/// The implementation uses AsynchronousFileChannel's position-based read operations
/// to ensure thread safety when multiple threads access the same file concurrently.
/// For partial reads, it uses memory mapping to avoid unnecessary data copying.
@TransportScheme("file")
public class FileByteRangeFetcher implements ChunkedTransportClient {

    /// The file path being accessed
    private final Path filePath;
    
    /// Asynchronous file channel for thread-safe concurrent access
    private final AsynchronousFileChannel asyncChannel;
    
    /// Cached size of the file (determined once at construction)
    private final AtomicReference<Long> cachedSize = new AtomicReference<>();
    
    /// Flag to track if this fetcher has been closed
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /// Creates a new file byte range fetcher for the specified path.
    /// 
    /// @param path The path to the local file
    /// @throws IllegalArgumentException if the path is null
    /// @throws IOException if the file cannot be accessed or opened
    public FileByteRangeFetcher(Path path) throws IOException {
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }
        
        // Validate file exists and is readable
        if (!Files.exists(path)) {
            throw new IOException("File does not exist: " + path);
        }
        
        if (!Files.isRegularFile(path)) {
            throw new IOException("Path is not a regular file: " + path);
        }
        
        if (!Files.isReadable(path)) {
            throw new IOException("File is not readable: " + path);
        }
        
        this.filePath = path;
        
        try {
            // Open the file with AsynchronousFileChannel for thread-safe access
            this.asyncChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
            
            // Cache the file size immediately
            this.cachedSize.set(Files.size(path));
        } catch (IOException e) {
            throw new IOException("Failed to open file: " + path, e);
        }
    }

    @Override
    public CompletableFuture<ByteBuffer> fetchRange(long offset, int length) throws IOException {
        validateNotClosed();
        
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("Length must be positive: " + length);
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Validate range is within file bounds
                long fileSize = cachedSize.get();
                if (offset >= fileSize) {
                    throw new RuntimeException("Offset " + offset + " is beyond file size " + fileSize);
                }
                
                // Adjust length if it would exceed file size
                long availableBytes = fileSize - offset;
                int actualLength = (int) Math.min(length, availableBytes);
                
                // For small reads, use direct ByteBuffer allocation and async read
                if (actualLength <= 8192) { // 8KB threshold
                    return readWithAsyncChannel(offset, actualLength);
                } else {
                    // For larger reads, use memory mapping for efficiency
                    return readWithMemoryMapping(offset, actualLength);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to fetch range [" + offset + "-" + (offset + length - 1) + "]", e);
            }
        });
    }

    @Override
    public CompletableFuture<Long> getSize() throws IOException {
        validateNotClosed();
        
        // Return cached size
        Long size = cachedSize.get();
        return CompletableFuture.completedFuture(size);
    }

    @Override
    public boolean supportsRangeRequests() {
        // File-based fetcher always supports range requests
        return true;
    }

    @Override
    public String getSource() {
        return filePath.toUri().toString();
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                asyncChannel.close();
            } catch (IOException e) {
                throw new IOException("Failed to close file channel for: " + filePath, e);
            }
        }
    }

    /// Reads data using AsynchronousFileChannel for small ranges.
    /// 
    /// @param offset The starting byte offset
    /// @param length The number of bytes to read
    /// @return ByteBuffer containing the requested data
    /// @throws IOException if the read operation fails
    private ByteBuffer readWithAsyncChannel(long offset, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        
        try {
            // Use the synchronous read method with absolute positioning
            int bytesRead = asyncChannel.read(buffer, offset).get();
            
            if (bytesRead != length) {
                throw new IOException("Expected to read " + length + " bytes but read " + bytesRead);
            }
            
            buffer.flip();
            return buffer;
        } catch (Exception e) {
            throw new IOException("Failed to read from async channel", e);
        }
    }

    /// Reads data using memory mapping for larger ranges.
    /// 
    /// @param offset The starting byte offset
    /// @param length The number of bytes to read
    /// @return ByteBuffer containing the requested data
    /// @throws IOException if the mapping operation fails
    private ByteBuffer readWithMemoryMapping(long offset, int length) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            // Map the requested region
            MappedByteBuffer mappedBuffer = fileChannel.map(
                FileChannel.MapMode.READ_ONLY, 
                offset, 
                length
            );
            
            // Create a new ByteBuffer with the data to avoid keeping the mapping alive
            ByteBuffer result = ByteBuffer.allocate(length);
            result.put(mappedBuffer);
            result.flip();
            
            return result;
        } catch (IOException e) {
            throw new IOException("Failed to memory map file region", e);
        }
    }

    /// Validates that this fetcher has not been closed.
    /// 
    /// @throws IOException if the fetcher has been closed
    private void validateNotClosed() throws IOException {
        if (closed.get()) {
            throw new IOException("FileByteRangeFetcher has been closed");
        }
    }
}