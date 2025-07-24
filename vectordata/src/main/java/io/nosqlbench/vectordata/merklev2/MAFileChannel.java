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

import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportIO;
import io.nosqlbench.nbdatatools.api.transport.FetchResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

///
/// A virtualized AsynchronousFileChannel that provides transparent access to remote content
/// with local caching and Merkle tree integrity verification.
///
/// This implementation:
/// 1. Virtualizes file size to match the remote content size
/// 2. Downloads and caches content chunks on demand
/// 3. Uses MerkleState to track verified chunks
/// 4. Provides transparent read access to verified content
///
/// ## Initialization Logic
/// When a MAFileChannel is created, the following preconditions and actions are performed:
/// 1. If there is no cache file and no mrkl state file and no mref reference file, then the
/// remote mref file is downloaded and then a mrkl state file is created from it, then the mref
/// reference file is discarded. Then the MAFileChannel is created with this state file and a new
///  cache content file channel.
/// 2. If there is a cache file and a mrkl state file, then they are both loaded as-is with no
/// modification into a new MAFileChannel.
/// 3. Any other initial state is considered invalid and an error should be thrown from the
/// constructor.
///
/// ## During Reads
/// When an MAFileChannel is read, each read operation has an initial precondition that the
/// matching mrkl state nodes must be valid. When there are matching leaf nodes which are not
/// valid, the content is downloaded and verified, and then the matching leaf node is marked valid.
/// This must all occur before the read to the underlying cache file is performed.
/// When the nodes are already marked as valid, this signals that the underlying cache file is
/// already valid to be read directly with no change to the affected chunks.
///
/// ## Merkle State Semantics
/// When a MerkleState is used with an MAFileChannel, the following semantics are enforced:
/// * The hash values for the merkle tree are never changed. Only the valid bits are changed.
/// * Bits can only be set to true. They cannot be set to false except during initialization.
/// * When a bit is set to true which is not already true, if it has a sibling and that sibling
/// is also set to true, then the parent bit is set to true before the call to change the bit
/// state returns. This should be non-blocking, as it should be an idempotent operation with
/// other overlapping bit changes.
///
public class MAFileChannel extends AsynchronousFileChannel {
    private final MerkleState merkleState;
    private final AsynchronousFileChannel localCache;
    private final ChunkedTransportClient transport;
    private final MerkleShape shape;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile boolean open = true;

    /**
     * Creates a new MAFileChannel according to the initialization logic specified in the class documentation.
     * 
     * @param localCachePath Path to local cache file
     * @param merkleStatePath Path to merkle state file  
     * @param remoteSource Remote source URL
     * @throws IOException If initialization fails or invalid state combination is encountered
     */
    public MAFileChannel(Path localCachePath, Path merkleStatePath, String remoteSource) throws IOException {
        // Ensure merkleStatePath ends with .mrkl (state file, not reference file)
        String statePathStr = merkleStatePath.toString();
        Path mrklStatePath;
        
        if (statePathStr.endsWith(".mrkl")) {
            mrklStatePath = merkleStatePath;
        } else if (statePathStr.endsWith(".mref")) {
            // If someone incorrectly passed a .mref path, convert it to .mrkl
            String baseName = statePathStr.substring(0, statePathStr.length() - 5); // Remove .mref
            mrklStatePath = merkleStatePath.resolveSibling(merkleStatePath.getFileName().toString().replace(".mref", ".mrkl"));
        } else {
            // For any other extension, append .mrkl
            mrklStatePath = merkleStatePath.resolveSibling(merkleStatePath.getFileName() + ".mrkl");
        }
            
        // Check current state to determine initialization path
        boolean cacheExists = java.nio.file.Files.exists(localCachePath);
        boolean mrklExists = java.nio.file.Files.exists(mrklStatePath);
        
        // Determine initialization path based on existing files
        MerkleState state;
        AsynchronousFileChannel cache;
        
        if (!cacheExists && !mrklExists) {
            // Case 1: No cache file and no mrkl state file
            // Download remote mref file, create mrkl state file from it, discard mref
            String refUrl = remoteSource + ".mref";
            Path tempRefPath = mrklStatePath.resolveSibling(mrklStatePath.getFileName() + ".tmp.mref");
            
            MerkleRef ref = downloadRef(refUrl, tempRefPath);
            state = MerkleState.fromRef(ref, mrklStatePath);
            
            // Clean up temp ref file
            java.nio.file.Files.deleteIfExists(tempRefPath);
            
            // Create new cache file
            cache = AsynchronousFileChannel.open(localCachePath,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                
        } else if (cacheExists && mrklExists) {
            // Case 2: Both cache file and mrkl state file exist
            // Load both as-is with no modification
            state = MerkleState.load(mrklStatePath);
            cache = AsynchronousFileChannel.open(localCachePath,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
                
        } else {
            // Case 3: Any other initial state is invalid
            String errorMsg = String.format(
                "Invalid initialization state: cache file %s (exists=%b), mrkl state file %s (exists=%b). " +
                "Either both files must exist or neither must exist.",
                localCachePath, cacheExists, mrklStatePath, mrklExists);
            throw new IOException(errorMsg);
        }
        
        // Create transport client
        ChunkedTransportClient transport = ChunkedTransportIO.create(remoteSource);
        
        // Initialize fields
        this.merkleState = state;
        this.localCache = cache;
        this.transport = transport;
        this.shape = state.getMerkleShape();
    }

    @Override
    public long size() throws IOException {
        return shape.getTotalContentSize();
    }

    @Override
    public <A> void read(ByteBuffer dst, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {
        if (!open) {
            handler.failed(new IOException("Channel is closed"), attachment);
            return;
        }

        CompletableFuture.supplyAsync(() -> {
            try {
                return readSync(dst, position);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).whenComplete((result, throwable) -> {
            if (throwable != null) {
                handler.failed(throwable instanceof IOException ? (IOException) throwable : new IOException(throwable), attachment);
            } else {
                handler.completed(result, attachment);
            }
        });
    }

    @Override
    public Future<Integer> read(ByteBuffer dst, long position) {
        if (!open) {
            return CompletableFuture.failedFuture(new IOException("Channel is closed"));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                return readSync(dst, position);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private int readSync(ByteBuffer dst, long position) throws IOException {
        lock.readLock().lock();
        try {
            if (!open) throw new IOException("Channel is closed");
            
            int originalLimit = dst.limit();
            int totalBytesRead = 0;
            long currentPosition = position;
            
            while (dst.hasRemaining() && currentPosition < size()) {
                int chunkIndex = shape.getChunkIndexForPosition(currentPosition);
                
                // Ensure chunk is available
                if (!merkleState.isValid(chunkIndex)) {
                    downloadAndVerifyChunk(chunkIndex);
                }
                
                // Calculate read parameters for this chunk
                long chunkStart = shape.getChunkStartPosition(chunkIndex);
                long chunkEnd = shape.getChunkEndPosition(chunkIndex);
                long readStart = Math.max(currentPosition, chunkStart);
                long readEnd = Math.min(currentPosition + dst.remaining(), chunkEnd);
                int readLength = (int) (readEnd - readStart);
                
                if (readLength <= 0) break;
                
                // Read from local cache
                ByteBuffer chunkBuffer = dst.slice();
                chunkBuffer.limit(readLength);
                
                Future<Integer> readFuture = localCache.read(chunkBuffer, readStart);
                int bytesRead = readFuture.get();
                
                if (bytesRead <= 0) break;
                
                dst.position(dst.position() + bytesRead);
                totalBytesRead += bytesRead;
                currentPosition += bytesRead;
            }
            
            return totalBytesRead;
        } catch (Exception e) {
            throw new IOException("Read operation failed", e);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void downloadAndVerifyChunk(int chunkIndex) throws IOException {
        long start = shape.getChunkStartPosition(chunkIndex);
        int length = (int) shape.getActualChunkSize(chunkIndex);
        
        // Download chunk from remote source
        CompletableFuture<? extends FetchResult<?>> downloadFuture = transport.fetchRange(start, length);
        FetchResult<?> result = downloadFuture.join();
        ByteBuffer chunkData = result.getData();
        
        // Verify and save using merkle state
        boolean saved = merkleState.saveIfValid(chunkIndex, chunkData, data -> {
            try {
                // Save to local cache
                localCache.write(data, start).get();
            } catch (Exception e) {
                throw new RuntimeException("Failed to save chunk to cache", e);
            }
        });
        
        if (!saved) {
            throw new IOException("Chunk " + chunkIndex + " failed verification");
        }
    }

    // Unsupported operations for read-only virtual file
    @Override
    public AsynchronousFileChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException("Truncate not supported on virtual file");
    }

    @Override
    public <A> void write(ByteBuffer src, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {
        handler.failed(new UnsupportedOperationException("Write not supported on virtual file"), attachment);
    }

    @Override
    public Future<Integer> write(ByteBuffer src, long position) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Write not supported on virtual file"));
    }

    @Override
    public void force(boolean metaData) throws IOException {
        if (open) {
            localCache.force(metaData);
            merkleState.flush();
        }
    }

    @Override
    public <A> void lock(long position, long size, boolean shared, A attachment, CompletionHandler<FileLock, ? super A> handler) {
        localCache.lock(position, size, shared, attachment, handler);
    }

    @Override
    public Future<FileLock> lock(long position, long size, boolean shared) {
        return localCache.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return localCache.tryLock(position, size, shared);
    }

    /**
     * Asynchronously prebuffers a range of bytes by ensuring all chunks containing the range are downloaded.
     * This method does not block and returns immediately with a CompletableFuture that completes when
     * all chunks in the range are available.
     *
     * @param position The starting position in the file
     * @param length The number of bytes to prebuffer
     * @return A CompletableFuture that completes when all chunks in the range are available
     */
    public CompletableFuture<Void> prebuffer(long position, long length) {
        if (position < 0) {
            return CompletableFuture.failedFuture(new IOException("Negative position: " + position));
        }
        if (length < 0) {
            return CompletableFuture.failedFuture(new IOException("Negative length: " + length));
        }

        return CompletableFuture.runAsync(() -> {
            try {
                long endPosition = Math.min(position + length, size());
                long currentPosition = position;
                
                while (currentPosition < endPosition) {
                    int chunkIndex = shape.getChunkIndexForPosition(currentPosition);
                    
                    // Ensure chunk is available
                    if (!merkleState.isValid(chunkIndex)) {
                        downloadAndVerifyChunk(chunkIndex);
                    }
                    
                    // Move to next chunk
                    currentPosition = shape.getChunkEndPosition(chunkIndex);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (open) {
                open = false;
                force(false);
                localCache.close();
                transport.close();
                merkleState.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static MerkleRef downloadRef(String refUrl, Path tempPath) throws IOException {
        // Download reference merkle tree
        ChunkedTransportClient client = ChunkedTransportIO.create(refUrl);
        try {
            long size = client.getSize().join();
            FetchResult<?> result = client.fetchRange(0, (int) size).join();
            ByteBuffer refData = result.getData();
            
            // Write to temp file and load as MerkleRef
            byte[] data = new byte[refData.remaining()];
            refData.get(data);
            java.nio.file.Files.createDirectories(tempPath.getParent());
            java.nio.file.Files.write(tempPath, data);
            return MerkleRef.load(tempPath);
        } finally {
            client.close();
        }
    }
}
