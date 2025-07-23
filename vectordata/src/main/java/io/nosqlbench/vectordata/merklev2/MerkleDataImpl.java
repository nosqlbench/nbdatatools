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

// BaseMerkleShape is now in the same package

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * A clean, memory-mapped implementation of MerkleData that serves as both reference and state.
 * 
 * File layout:
 * [Hash Data Region] [BitSet Region] [Footer Region]
 * 
 * The implementation uses absolute positioning for all I/O operations to ensure thread safety.
 * Memory barriers and locks provide additional synchronization where needed.
 */
public class MerkleDataImpl implements MerkleData {
    private static final int HASH_SIZE = 32; // SHA-256
    private final MerkleShape shape;
    private final FileChannel channel;
    private final BitSet validChunks;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    volatile boolean closed = false;
    
    // For in-memory data trees (when created from data instead of file)
    private final byte[][] hashes;
    private final boolean isFileChannel;

    // Public access for testing
    public boolean closed() {
        return closed;
    }
    
    /**
     * Creates a new MerkleDataImpl from a data file with progress tracking.
     * 
     * @param dataPath The path to the data file
     * @return A MerkleRefBuildProgress that tracks the building process
     * @throws IOException If an I/O error occurs
     */
    public static MerkleRefBuildProgress fromDataWithProgress(Path dataPath) throws IOException {
        FileChannel channel = FileChannel.open(dataPath, StandardOpenOption.READ);
        long fileSize = channel.size();
        
        // Create shape based on file size
        MerkleShape shape = new BaseMerkleShape(fileSize);
        
        // Create progress tracker
        MerkleRefBuildProgress progress = new MerkleRefBuildProgress(shape.getLeafCount(), fileSize);
        
        // Start the build process asynchronously
        CompletableFuture.runAsync(() -> {
            try {
                progress.setStage(MerkleRefBuildProgress.Stage.LEAF_NODE_PROCESSING);
                
                // Create hashes array
                byte[][] hashes = new byte[shape.getNodeCount()][];
                
                // Create executor service for parallel processing
                ExecutorService executor = Executors.newFixedThreadPool(
                    Math.min(Runtime.getRuntime().availableProcessors(), 16));
                
                try {
                    // Process chunks in parallel
                    CompletableFuture<?>[] chunkFutures = new CompletableFuture[shape.getLeafCount()];
                    for (int i = 0; i < shape.getLeafCount(); i++) {
                        final int leafIndex = i;
                        chunkFutures[i] = CompletableFuture.runAsync(() -> {
                            try {
                                long startTime = System.nanoTime();
                                
                                // Get chunk boundaries
                                long start = shape.getChunkStartPosition(leafIndex);
                                long end = shape.getChunkEndPosition(leafIndex);
                                long size = end - start;
                                
                                // Read chunk data
                                ByteBuffer buffer = ByteBuffer.allocate((int) size);
                                synchronized (channel) {
                                    channel.read(buffer, start);
                                }
                                buffer.flip();
                                
                                // Hash chunk data
                                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                                digest.update(buffer);
                                byte[] hash = digest.digest();
                                
                                // Store hash
                                int hashIndex = shape.getOffset() + leafIndex;
                                hashes[hashIndex] = hash;
                                
                                long endTime = System.nanoTime();
                                progress.addHashComputeTime(endTime - startTime);
                                progress.incrementProcessedChunks();
                            } catch (Exception e) {
                                throw new RuntimeException("Error processing chunk " + leafIndex, e);
                            }
                        }, executor);
                    }
                    
                    // When all chunks are processed, compute internal nodes and complete future
                    CompletableFuture.allOf(chunkFutures).thenRun(() -> {
                        try {
                            progress.setStage(MerkleRefBuildProgress.Stage.INTERNAL_NODE_PROCESSING);
                            long startTime = System.nanoTime();
                            
                            // Compute internal nodes
                            computeInternalNodes(hashes, shape);
                            
                            long endTime = System.nanoTime();
                            progress.addInternalNodeComputeTime(endTime - startTime);
                            
                            // Create and return MerkleDataImpl
                            MerkleDataImpl ref = new MerkleDataImpl(shape, hashes);
                            progress.complete(ref);
                        } catch (Exception e) {
                            progress.completeExceptionally(e);
                        } finally {
                            executor.shutdown();
                            try {
                                channel.close();
                            } catch (IOException e) {
                                // Log but don't fail the whole operation
                            }
                        }
                    });
                } catch (Exception e) {
                    executor.shutdown();
                    throw e;
                }
            } catch (Exception e) {
                progress.completeExceptionally(e);
                try {
                    channel.close();
                } catch (IOException ioException) {
                    // Log but don't fail the whole operation
                }
            }
        });
        
        return progress;
    }
    
    /**
     * Creates a new MerkleDataImpl from a data file.
     * 
     * @param dataPath The path to the data file
     * @return A CompletableFuture that will complete with the MerkleDataImpl
     * @throws IOException If an I/O error occurs
     */
    public static CompletableFuture<MerkleDataImpl> fromData(Path dataPath) throws IOException {
        FileChannel channel = FileChannel.open(dataPath, StandardOpenOption.READ);
        long fileSize = channel.size();
        
        // Create shape based on file size
        MerkleShape shape = new BaseMerkleShape(fileSize);
        
        // Create hashes array
        byte[][] hashes = new byte[shape.getNodeCount()][];
        
        // Create executor service for parallel processing
        ExecutorService executor = Executors.newFixedThreadPool(
            Math.min(Runtime.getRuntime().availableProcessors(), 16));
        
        CompletableFuture<MerkleDataImpl> future = new CompletableFuture<>();
        
        // Process chunks in parallel
        CompletableFuture<?>[] chunkFutures = new CompletableFuture[shape.getLeafCount()];
        for (int i = 0; i < shape.getLeafCount(); i++) {
            final int leafIndex = i;
            chunkFutures[i] = CompletableFuture.runAsync(() -> {
                try {
                    // Get chunk boundaries
                    long start = shape.getChunkStartPosition(leafIndex);
                    long end = shape.getChunkEndPosition(leafIndex);
                    long size = end - start;
                    
                    // Read chunk data
                    ByteBuffer buffer = ByteBuffer.allocate((int) size);
                    synchronized (channel) {
                        channel.read(buffer, start);
                    }
                    buffer.flip();
                    
                    // Hash chunk data
                    MessageDigest digest = MessageDigest.getInstance("SHA-256");
                    digest.update(buffer);
                    byte[] hash = digest.digest();
                    
                    // Store hash
                    int hashIndex = shape.getOffset() + leafIndex;
                    hashes[hashIndex] = hash;
                } catch (Exception e) {
                    throw new RuntimeException("Error processing chunk " + leafIndex, e);
                }
            }, executor);
        }
        
        // When all chunks are processed, compute internal nodes and complete future
        CompletableFuture.allOf(chunkFutures).thenRun(() -> {
            try {
                // Compute internal nodes
                computeInternalNodes(hashes, shape);
                
                // Create and return MerkleDataImpl
                MerkleDataImpl ref = new MerkleDataImpl(shape, hashes);
                future.complete(ref);
                
                // Close channel
                channel.close();
            } catch (Exception e) {
                future.completeExceptionally(e);
            } finally {
                executor.shutdown();
            }
        });
        
        return future;
    }
    
    /**
     * Creates a new MerkleDataImpl from a ByteBuffer.
     * 
     * @param data The data buffer
     * @return A CompletableFuture that will complete with the MerkleDataImpl
     */
    public static CompletableFuture<MerkleDataImpl> fromData(ByteBuffer data) {
        // Create shape based on buffer size
        MerkleShape shape = new BaseMerkleShape(data.remaining());
        
        // Create hashes array
        byte[][] hashes = new byte[shape.getNodeCount()][];
        
        // Create executor service for parallel processing
        ExecutorService executor = Executors.newFixedThreadPool(
            Math.min(Runtime.getRuntime().availableProcessors(), 16));
        
        CompletableFuture<MerkleDataImpl> future = new CompletableFuture<>();
        
        // Process chunks in parallel
        CompletableFuture<?>[] chunkFutures = new CompletableFuture[shape.getLeafCount()];
        for (int i = 0; i < shape.getLeafCount(); i++) {
            final int leafIndex = i;
            chunkFutures[i] = CompletableFuture.runAsync(() -> {
                try {
                    // Get chunk boundaries
                    long start = shape.getChunkStartPosition(leafIndex);
                    long end = shape.getChunkEndPosition(leafIndex);
                    long size = end - start;
                    
                    // Create a slice of the buffer for this chunk
                    ByteBuffer chunkBuffer = data.duplicate();
                    chunkBuffer.position((int) start);
                    chunkBuffer.limit((int) end);
                    
                    // Hash chunk data
                    MessageDigest digest = MessageDigest.getInstance("SHA-256");
                    digest.update(chunkBuffer);
                    byte[] hash = digest.digest();
                    
                    // Store hash
                    int hashIndex = shape.getOffset() + leafIndex;
                    hashes[hashIndex] = hash;
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("SHA-256 algorithm not available", e);
                }
            }, executor);
        }
        
        // When all chunks are processed, compute internal nodes and complete future
        CompletableFuture.allOf(chunkFutures).thenRun(() -> {
            try {
                // Compute internal nodes
                computeInternalNodes(hashes, shape);
                
                // Create and return MerkleDataImpl
                MerkleDataImpl ref = new MerkleDataImpl(shape, hashes);
                future.complete(ref);
            } catch (Exception e) {
                future.completeExceptionally(e);
            } finally {
                executor.shutdown();
            }
        });
        
        return future;
    }
    
    /**
     * Creates an empty MerkleDataImpl for the given content size.
     * All hashes will be null/empty.
     * 
     * @param contentSize The total size of the content
     * @return An empty MerkleDataImpl
     */
    public static MerkleDataImpl createEmpty(long contentSize) {
        MerkleShape shape = new BaseMerkleShape(contentSize);
        byte[][] emptyHashes = new byte[shape.getNodeCount()][];
        return new MerkleDataImpl(shape, emptyHashes);
    }
    
    /**
     * Creates a MerkleDataImpl from pre-computed hashes.
     * 
     * @param shape The merkle shape
     * @param hashes The pre-computed hashes
     * @return A MerkleDataImpl with the given hashes
     */
    public static MerkleDataImpl createFromHashes(MerkleShape shape, byte[][] hashes) {
        return new MerkleDataImpl(shape, hashes);
    }
    
    /**
     * Creates a MerkleDataImpl from pre-computed hashes and BitSet.
     * 
     * @param shape The merkle shape
     * @param hashes The pre-computed hashes
     * @param validChunks The BitSet indicating which chunks are valid
     * @return A MerkleDataImpl with the given hashes and BitSet
     */
    public static MerkleDataImpl createFromHashesAndBitSet(MerkleShape shape, byte[][] hashes, BitSet validChunks) {
        return new MerkleDataImpl(shape, hashes, validChunks);
    }
    
    /**
     * Computes the internal nodes of the merkle tree.
     * 
     * @param hashes The array of hashes
     * @param shape The merkle shape
     * @throws NoSuchAlgorithmException If the SHA-256 algorithm is not available
     */
    private static void computeInternalNodes(byte[][] hashes, MerkleShape shape) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        
        // Start from the bottom of the tree and work up
        for (int i = shape.getOffset() - 1; i >= 0; i--) {
            // Get left and right child indices
            int leftChild = 2 * i + 1;
            int rightChild = 2 * i + 2;
            
            // Get left and right child hashes
            byte[] leftHash = hashes[leftChild];
            byte[] rightHash = hashes[rightChild];
            
            // If right child is beyond the tree, use left child hash
            if (rightChild >= shape.getNodeCount()) {
                rightHash = leftHash;
            }
            
            // Handle null hashes (skip internal node computation if children are null)
            if (leftHash == null || rightHash == null) {
                continue;
            }
            
            // Combine hashes
            digest.reset();
            digest.update(leftHash);
            digest.update(rightHash);
            hashes[i] = digest.digest();
        }
    }

    /**
     * Creates a new MerkleDataImpl from an existing file.
     */
    public static MerkleDataImpl load(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // Read footer to get shape information
        MerkleShape shape = readRefStateShapeFromFooter(channel);

        // Read valid chunks bitset
        BitSet validChunks = readStateValidChunksFromFile(channel, shape);

        return new MerkleDataImpl(shape, channel, validChunks);
    }

    /**
     * Creates a new MerkleDataImpl file from a reference tree.
     */
    public static MerkleDataImpl createFromRef(MerkleRef ref, Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, 
            StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

        MerkleShape shape = ref.getShape();

        // Copy all hashes from reference
        copyHashesFromRefToState(channel, ref, shape);

        // Initialize bitset - all false (unverified)
        BitSet validChunks = new BitSet(shape.getLeafCount());
        writeStateValidChunksToFile(channel, validChunks, shape);

        // Write footer
        writeRefStateShapeToFooter(channel, shape);

        return new MerkleDataImpl(shape, channel, validChunks);
    }

    private MerkleDataImpl(MerkleShape shape, FileChannel channel, BitSet validChunks) {
        this.shape = shape;
        this.channel = channel;
        this.validChunks = validChunks;
        this.hashes = null;
        this.isFileChannel = true;
    }
    
    /**
     * Creates a new MerkleDataImpl with the given shape and hashes (in-memory).
     * 
     * @param shape The merkle shape
     * @param hashes The array of hashes
     */
    private MerkleDataImpl(MerkleShape shape, byte[][] hashes) {
        this.shape = shape;
        this.channel = null;
        this.validChunks = new BitSet(shape.getLeafCount());
        // For in-memory trees created from data, all chunks are valid
        this.validChunks.set(0, shape.getLeafCount());
        this.hashes = hashes;
        this.isFileChannel = false;
    }
    
    /**
     * Creates a new MerkleDataImpl with the given shape, hashes, and BitSet (in-memory).
     * 
     * @param shape The merkle shape
     * @param hashes The array of hashes
     * @param validChunks The BitSet indicating which chunks are valid
     */
    private MerkleDataImpl(MerkleShape shape, byte[][] hashes, BitSet validChunks) {
        this.shape = shape;
        this.channel = null;
        this.validChunks = validChunks;
        this.hashes = hashes;
        this.isFileChannel = false;
    }

    // MerkleRef interface implementation

    @Override
    public MerkleShape getShape() {
        return shape;
    }

    @Override
    public byte[] getHashForLeaf(int leafIndex) {
        lock.readLock().lock();
        try {
            if (closed) return null;

            shape.validateChunkIndex(leafIndex);

            if (isFileChannel) {
                // Calculate position in hash data region
                long hashPosition = (long) (shape.getOffset() + leafIndex) * HASH_SIZE;

                ByteBuffer hashBuffer = ByteBuffer.allocate(HASH_SIZE);
                int bytesRead = channel.read(hashBuffer, hashPosition);

                if (bytesRead != HASH_SIZE) return null;

                return hashBuffer.array();
            } else {
                // In-memory implementation
                int hashIndex = shape.getOffset() + leafIndex;
                return hashes[hashIndex];
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read hash for leaf " + leafIndex, e);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public byte[] getHashForIndex(int index) {
        lock.readLock().lock();
        try {
            if (closed) return null;

            if (index < 0 || index >= shape.getNodeCount()) return null;

            if (isFileChannel) {
                // Calculate position in hash data region
                long hashPosition = (long) index * HASH_SIZE;

                ByteBuffer hashBuffer = ByteBuffer.allocate(HASH_SIZE);
                int bytesRead = channel.read(hashBuffer, hashPosition);

                if (bytesRead != HASH_SIZE) return null;

                return hashBuffer.array();
            } else {
                // In-memory implementation
                return hashes[index];
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read hash for index " + index, e);
        } finally {
            lock.readLock().unlock();
        }
    }

    // MerkleState interface implementation
    @Override
    public boolean saveIfValid(int chunkIndex, ByteBuffer data, Consumer<ByteBuffer> saveCallback) {
        lock.writeLock().lock();
        try {
            if (closed) return false;

            shape.validateChunkIndex(chunkIndex);

            // Verify against reference hash by computing hash and comparing
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                byte[] actualHash = digest.digest(data.array());
                byte[] expectedHash = getHashForLeaf(chunkIndex);

                if (expectedHash == null || !java.util.Arrays.equals(actualHash, expectedHash)) {
                    return false;
                }
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("SHA-256 algorithm not available", e);
            }

            // Save data using callback
            try {
                saveCallback.accept(data.duplicate());
            } catch (Exception e) {
                throw new RuntimeException("Save callback failed for chunk " + chunkIndex, e);
            }

            // Mark as valid
            validChunks.set(chunkIndex);

            // Persist valid chunks to file (only for file-based implementations)
            if (isFileChannel) {
                writeStateValidChunksToFile(channel, validChunks, shape);
                // Force to disk
                flush();
            }

            return true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to save chunk " + chunkIndex, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public MerkleShape getMerkleShape() {
        return shape;
    }

    @Override
    public BitSet getValidChunks() {
        lock.readLock().lock();
        try {
            return (BitSet) validChunks.clone();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean isValid(int chunkIndex) {
        lock.readLock().lock();
        try {
            if (closed) return false;
            shape.validateChunkIndex(chunkIndex);
            return validChunks.get(chunkIndex);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() {
        lock.readLock().lock();
        try {
            if (!closed && isFileChannel) {
                channel.force(false);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush", e);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public java.util.List<byte[]> getPathToRoot(int leafIndex) {
        lock.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("Cannot get path from a closed MerkleDataImpl");
            }
            
            if (leafIndex < 0 || leafIndex >= shape.getLeafCount()) {
                throw new IllegalArgumentException("Invalid leaf index: " + leafIndex + 
                    ", must be between 0 and " + (shape.getLeafCount() - 1));
            }

            java.util.List<byte[]> path = new java.util.ArrayList<>();

            // Start with the leaf node
            int nodeIndex = shape.getOffset() + leafIndex;

            // Add the leaf hash to the path
            byte[] leafHash = getHashForIndex(nodeIndex);
            if (leafHash != null) {
                path.add(leafHash.clone()); // Clone to prevent external modification
            } else {
                throw new IllegalStateException("Hash not available for leaf " + leafIndex);
            }

            // Traverse up the tree to the root
            while (nodeIndex > 0) {
                // Move to parent node
                nodeIndex = (nodeIndex - 1) / 2;

                // Add the parent hash to the path
                byte[] parentHash = getHashForIndex(nodeIndex);
                if (parentHash != null) {
                    path.add(parentHash.clone()); // Clone to prevent external modification
                } else {
                    throw new IllegalStateException("Hash not available for node " + nodeIndex);
                }
            }

            return path;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            if (!closed) {
                closed = true;
                if (isFileChannel) {
                    flush();
                    channel.close();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to close", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public MerkleRef toRef() {
        lock.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("MerkleState is closed");
            }
            
            // Check if all chunks are validated
            int totalChunks = shape.getLeafCount();
            int validCount = validChunks.cardinality();
            
            if (validCount != totalChunks) {
                throw new IncompleteMerkleStateException(validCount, totalChunks);
            }
            
            // All chunks are validated - return this instance as MerkleRef interface
            return this;
            
        } finally {
            lock.readLock().unlock();
        }
    }

    // Helper methods for file I/O
    private static void copyHashesFromRefToState(FileChannel channel, MerkleRef ref, MerkleShape shape) throws IOException {
        // Copy all node hashes
        for (int i = 0; i < shape.getNodeCount(); i++) {
            byte[] hash = ref.getHashForIndex(i);
            if (hash != null) {
                ByteBuffer hashBuffer = ByteBuffer.wrap(hash);
                long hashPosition = (long) i * HASH_SIZE;
                channel.write(hashBuffer, hashPosition);
            }
        }
    }

    private static void writeStateValidChunksToFile(FileChannel channel, BitSet validChunks, MerkleShape shape) throws IOException {
        long bitsetPosition = (long) shape.getNodeCount() * HASH_SIZE;

        // Convert BitSet to byte array
        byte[] bitsetBytes = validChunks.toByteArray();
        int requiredBytes = (shape.getLeafCount() + 7) / 8; // Round up to byte boundary

        // Ensure we have the right size
        byte[] paddedBytes = new byte[requiredBytes];
        System.arraycopy(bitsetBytes, 0, paddedBytes, 0, Math.min(bitsetBytes.length, requiredBytes));

        ByteBuffer bitsetBuffer = ByteBuffer.wrap(paddedBytes);
        channel.write(bitsetBuffer, bitsetPosition);
    }

    private static BitSet readStateValidChunksFromFile(FileChannel channel, MerkleShape shape) throws IOException {
        long bitsetPosition = (long) shape.getNodeCount() * HASH_SIZE;
        int requiredBytes = (shape.getLeafCount() + 7) / 8;

        ByteBuffer bitsetBuffer = ByteBuffer.allocate(requiredBytes);
        channel.read(bitsetBuffer, bitsetPosition);

        return BitSet.valueOf(bitsetBuffer.array());
    }

    private static void writeRefStateShapeToFooter(FileChannel channel, MerkleShape shape) throws IOException {
        // Use Merklev2Footer to serialize shape parameters to footer
        long footerPosition = (long) shape.getNodeCount() * HASH_SIZE + ((shape.getLeafCount() + 7) / 8);

        Merklev2Footer footer = Merklev2Footer.create(shape);
        footer.writeToChannel(channel, footerPosition);
    }

    private static MerkleShape readRefStateShapeFromFooter(FileChannel channel) throws IOException {
        // Read footer from end
        long fileSize = channel.size();
        long footerPosition = fileSize - Merklev2Footer.FIXED_FOOTER_SIZE;

        // Read the footer using Merklev2Footer
        Merklev2Footer footer = Merklev2Footer.readFromChannel(channel, footerPosition);

        // Convert footer to MerkleShape
        return footer.toMerkleShape();
    }
    
    /**
     * Saves this merkle tree to a file.
     * 
     * @param path The path to save the tree to
     * @throws IOException If an I/O error occurs during saving
     */
    public void save(Path path) throws IOException {
        if (closed) {
            throw new IllegalStateException("Cannot save a closed MerkleDataImpl");
        }
        
        // Create a BitSet with all bits set to true for in-memory trees, or use validChunks for file-based
        BitSet bitsToSave = isFileChannel ? (BitSet) validChunks.clone() : new BitSet(shape.getLeafCount());
        if (!isFileChannel) {
            bitsToSave.set(0, shape.getLeafCount());
        }
        
        try (FileChannel channel = FileChannel.open(
            path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        )) {
            // Write hash data in the same order as MerkleTree: leaves first, then internal nodes
            // Write leaf hashes
            for (int i = 0; i < shape.getLeafCount(); i++) {
                int hashIndex = shape.getOffset() + i;
                byte[] hash = getHashForIndex(hashIndex);
                if (hash == null) {
                    hash = new byte[HASH_SIZE];
                }
                long leafPosition = (long) (shape.getOffset() + i) * HASH_SIZE;
                channel.write(ByteBuffer.wrap(hash), leafPosition);
            }
            
            // Write padded leaves (if any)
            byte[] zeroHash = new byte[HASH_SIZE];
            for (int i = shape.getLeafCount(); i < shape.getCapLeaf(); i++) {
                long paddedPosition = (long) (shape.getOffset() + i) * HASH_SIZE;
                channel.write(ByteBuffer.wrap(zeroHash), paddedPosition);
            }
            
            // Write internal node hashes
            for (int i = 0; i < shape.getOffset(); i++) {
                byte[] hash = getHashForIndex(i);
                if (hash == null) {
                    hash = new byte[HASH_SIZE];
                }
                long internalPosition = (long) i * HASH_SIZE;
                channel.write(ByteBuffer.wrap(hash), internalPosition);
            }
            
            // Write BitSet data
            byte[] bitSetData = bitsToSave.toByteArray();
            long bitSetPosition = (long) shape.getNodeCount() * HASH_SIZE;
            channel.write(ByteBuffer.wrap(bitSetData), bitSetPosition);
            
            // Write footer using the Merklev2Footer
            Merklev2Footer footer = Merklev2Footer.create(
                shape.getChunkSize(),
                shape.getTotalContentSize(),
                shape.getTotalChunks(),
                shape.getLeafCount(),
                shape.getCapLeaf(),
                shape.getNodeCount(),
                shape.getOffset(),
                shape.getInternalNodeCount(),
                bitSetData.length
            );
            long footerPosition = bitSetPosition + bitSetData.length;
            channel.write(footer.toByteBuffer(), footerPosition);
            
            // Force all changes to disk
            channel.force(true);
        }
    }
    
    /**
     * Gets the number of leaves in this merkle tree.
     * 
     * @return The number of leaf nodes
     */
    public int getNumberOfLeaves() {
        if (closed) {
            throw new IllegalStateException("Cannot get number of leaves from a closed MerkleDataImpl");
        }
        return shape.getLeafCount();
    }
    
    /**
     * Gets the chunk size used by this merkle tree.
     * 
     * @return The chunk size in bytes
     */
    public long getChunkSize() {
        if (closed) {
            throw new IllegalStateException("Cannot get chunk size from a closed MerkleDataImpl");
        }
        return shape.getChunkSize();
    }
    
    /**
     * Gets the total size of the data this merkle tree represents.
     * 
     * @return The total size in bytes
     */
    public long totalSize() {
        if (closed) {
            throw new IllegalStateException("Cannot get total size from a closed MerkleDataImpl");
        }
        return shape.getTotalContentSize();
    }
    
    /**
     * Gets the hash for any node in the merkle tree by its tree index.
     * This is an alias for getHashForIndex to maintain compatibility.
     * 
     * @param nodeIndex The tree index of the node
     * @return The hash bytes for the specified node
     * @throws IllegalArgumentException if nodeIndex is out of bounds
     */
    public byte[] getHash(int nodeIndex) {
        return getHashForIndex(nodeIndex);
    }
    
    /**
     * Finds mismatched chunks between this MerkleDataImpl and another.
     * Returns a list of chunk indices where the leaf hashes differ.
     * 
     * @param other The other MerkleDataImpl to compare with
     * @return A list of mismatched chunk indices
     */
    public java.util.List<MerkleMismatch> findMismatchedChunks(MerkleDataImpl other) {
        java.util.List<MerkleMismatch> mismatches = new java.util.ArrayList<>();
        
        if (other == null || closed || other.closed) {
            return mismatches;
        }
        
        // Only compare if shapes are compatible
        if (!shape.equals(other.shape)) {
            // If shapes don't match, consider all chunks as mismatched
            for (int i = 0; i < shape.getLeafCount(); i++) {
                long startOffset = shape.getChunkStartPosition(i);
                long endOffset = shape.getChunkEndPosition(i);
                long length = endOffset - startOffset;
                mismatches.add(new MerkleMismatch(i, startOffset, length));
            }
            return mismatches;
        }
        
        // Compare leaf hashes only
        for (int leafIndex = 0; leafIndex < shape.getLeafCount(); leafIndex++) {
            byte[] thisHash = getHashForLeaf(leafIndex);
            byte[] otherHash = other.getHashForLeaf(leafIndex);
            
            boolean hashesMatch = false;
            if (thisHash == null && otherHash == null) {
                hashesMatch = true;
            } else if (thisHash != null && otherHash != null) {
                hashesMatch = java.util.Arrays.equals(thisHash, otherHash);
            }
            
            if (!hashesMatch) {
                long startOffset = shape.getChunkStartPosition(leafIndex);
                long endOffset = shape.getChunkEndPosition(leafIndex);
                long length = endOffset - startOffset;
                mismatches.add(new MerkleMismatch(leafIndex, startOffset, length));
            }
        }
        
        return mismatches;
    }
    
    /**
     * Compares this MerkleDataImpl with another for equality.
     * Two MerkleDataImpl instances are equal if they have the same shape and all hashes match.
     * 
     * @param other The other MerkleDataImpl to compare with
     * @return true if the merkle trees are equal, false otherwise
     */
    public boolean equals(MerkleDataImpl other) {
        if (other == null || closed || other.closed) {
            return false;
        }
        
        // Check if shapes are compatible
        if (!shape.equals(other.shape)) {
            return false;
        }
        
        // Compare all hashes
        for (int i = 0; i < shape.getNodeCount(); i++) {
            byte[] thisHash = getHashForIndex(i);
            byte[] otherHash = other.getHashForIndex(i);
            
            if (thisHash == null && otherHash == null) {
                continue;
            }
            if (thisHash == null || otherHash == null) {
                return false;
            }
            if (!java.util.Arrays.equals(thisHash, otherHash)) {
                return false;
            }
        }
        
        return true;
    }
    
    @Override
    public MerkleState createEmptyState(Path path) throws IOException {
        return createFromRef(this, path);
    }
}
