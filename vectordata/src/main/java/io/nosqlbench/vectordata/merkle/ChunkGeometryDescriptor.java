package io.nosqlbench.vectordata.merkle;

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

/**
 * Authoritative source for all chunk-related calculations in the merkle system.
 * This class provides the single source of truth for chunk boundaries, indices,
 * and size calculations to ensure consistency across all merkle components.
 * 
 * All merkle classes MUST use this for chunk calculations to prevent the
 * inconsistencies that caused the CohereAccessTest failure.
 * 
 * The chunk size is automatically calculated based on content size to maintain
 * optimal chunk counts while staying within power-of-2 constraints.
 */
public final class ChunkGeometryDescriptor {

    /** Minimum chunk size: 2^20 = 1MB */
    private static final long MIN_CHUNK_SIZE = 1L << 20; // 1MB

    /** Maximum chunk size: 64MB */
    private static final long MAX_CHUNK_SIZE = 64L * 1024L * 1024L; // 64MB

    /** Maximum desired number of chunks before scaling up chunk size */
    private static final int MAX_PREFERRED_CHUNKS = 4096;

    private final long chunkSize;
    private final long totalContentSize;
    private final int totalChunks;
    private final int leafCount;
    private final int capLeaf;
    private final int nodeCount;
    private final int offset;
    private final int internalNodeCount;

    /**
     * Creates a new ChunkGeometryDescriptor with automatically calculated chunk size.
     * The chunk size is determined to keep the number of chunks at or below 4096
     * while using power-of-2 sizes between 1MB and 64MB.
     * 
     * @param totalContentSize The total size of the content in bytes (must be non-negative)
     * @throws IllegalArgumentException if contentSize is negative
     */
    public ChunkGeometryDescriptor(long totalContentSize) {
        this.totalContentSize = validateContentSize(totalContentSize);
        this.chunkSize = calculateOptimalChunkSize(totalContentSize);
        this.totalChunks = calculateTotalChunks(totalContentSize, chunkSize);
        this.leafCount = totalChunks;
        this.capLeaf = calculateCapLeaf();
        this.nodeCount = calculateNodeCount(capLeaf);
        this.offset = calculateOffset(capLeaf);
        this.internalNodeCount = calculateInternalNodeCount(nodeCount, leafCount);
    }

    /**
     * Creates a new ChunkGeometryDescriptor with a specific chunk size.
     * This is primarily for testing purposes where specific chunk sizes are needed.
     * 
     * @param totalContentSize The total content size in bytes
     * @param chunkSize The specific chunk size to use
     * @throws IllegalArgumentException if chunkSize is not positive
     */
    public ChunkGeometryDescriptor(long totalContentSize, long chunkSize) {
        this.totalContentSize = validateContentSize(totalContentSize);
        this.chunkSize = validateChunkSize(chunkSize);
        this.totalChunks = calculateTotalChunks(totalContentSize, chunkSize);
        this.leafCount = totalChunks;
        this.capLeaf = calculateCapLeaf();
        this.nodeCount = calculateNodeCount(capLeaf);
        this.offset = calculateOffset(capLeaf);
        this.internalNodeCount = calculateInternalNodeCount(nodeCount, leafCount);
    }

    /**
     * Private constructor for internal use only.
     * This is used to create a new instance with all values specified.
     */
    private ChunkGeometryDescriptor(
            long chunkSize, 
            long totalContentSize, 
            int totalChunks,
            int leafCount,
            int capLeaf,
            int nodeCount,
            int offset,
            int internalNodeCount) {
        this.chunkSize = chunkSize;
        this.totalContentSize = totalContentSize;
        this.totalChunks = totalChunks;
        this.leafCount = leafCount;
        this.capLeaf = capLeaf;
        this.nodeCount = nodeCount;
        this.offset = offset;
        this.internalNodeCount = internalNodeCount;
    }

    /**
     * Creates a ChunkGeometryDescriptor for the specified total content size.
     * This is the single entry point for obtaining a descriptor based on content size.
     *
     * @param totalContentSize The total size of the content in bytes (must be non-negative)
     * @return A ChunkGeometryDescriptor capturing all geometry and Merkle tree dimensions
     * @throws IllegalArgumentException if totalContentSize is negative
     */
    public static ChunkGeometryDescriptor fromContentSize(long totalContentSize) {
        return new ChunkGeometryDescriptor(totalContentSize);
    }

    /**
     * Gets the chunk boundary information for the specified chunk index.
     * This is the SINGLE AUTHORITATIVE calculation for chunk boundaries.
     * 
     * @param chunkIndex The index of the chunk (0-based)
     * @return ChunkBoundary containing start, end, and size information
     * @throws IllegalArgumentException if chunkIndex is out of bounds
     */
    public ChunkBoundary getChunkBoundary(int chunkIndex) {
        validateChunkIndex(chunkIndex);

        long startInclusive = (long) chunkIndex * chunkSize;
        long endExclusive = Math.min(startInclusive + chunkSize, totalContentSize);

        return new ChunkBoundary(chunkIndex, startInclusive, endExclusive);
    }

    /**
     * Gets the chunk index that contains the specified content position.
     * This is the SINGLE AUTHORITATIVE calculation for position-to-chunk mapping.
     * 
     * @param contentPosition The position in the content (0-based)
     * @return The chunk index containing this position
     * @throws IllegalArgumentException if contentPosition is out of bounds
     */
    public int getChunkIndexForPosition(long contentPosition) {
        validateContentPosition(contentPosition);

        long chunkIndex = contentPosition / chunkSize;
        // Clamp to valid range to handle edge cases
        return Math.min((int) chunkIndex, totalChunks - 1);
    }

    /**
     * Validates that a chunk index is within the valid range.
     * 
     * @param chunkIndex The chunk index to validate
     * @throws IllegalArgumentException if the index is out of bounds
     */
    public void validateChunkIndex(int chunkIndex) {
        if (chunkIndex < 0 || chunkIndex >= totalChunks) {
            throw new IllegalArgumentException(
                "Chunk index " + chunkIndex + " out of bounds [0, " + totalChunks + ")");
        }
    }

    /**
     * Validates that a content position is within the valid range.
     * 
     * @param contentPosition The content position to validate
     * @throws IllegalArgumentException if the position is out of bounds
     */
    public void validateContentPosition(long contentPosition) {
        if (contentPosition < 0) {
            throw new IllegalArgumentException("Content position cannot be negative: " + contentPosition);
        }
        if (contentPosition >= totalContentSize) {
            throw new IllegalArgumentException(
                "Content position " + contentPosition + " exceeds content size " + totalContentSize);
        }
    }

    /**
     * Legacy method for compatibility with existing code.
     * 
     * @param filePosition The file position to validate
     * @throws IllegalArgumentException if the position is out of bounds
     * @deprecated Use validateContentPosition() instead
     */
    @Deprecated
    public void validateFilePosition(long filePosition) {
        validateContentPosition(filePosition);
    }

    /**
     * Calculates the optimal chunk size for the given content size.
     * 
     * Algorithm:
     * 1. Start with minimum chunk size (1MB)
     * 2. While chunks would exceed 4096 and chunk size < 64MB, double the chunk size
     * 3. Use the resulting chunk size
     * 
     * @param contentSize The total content size
     * @return The optimal chunk size (power of 2)
     */
    private static long calculateOptimalChunkSize(long contentSize) {
        if (contentSize == 0) {
            return MIN_CHUNK_SIZE;
        }

        long chunkSize = MIN_CHUNK_SIZE;

        // Scale up chunk size to keep chunk count reasonable
        while (chunkSize < MAX_CHUNK_SIZE) {
            int chunkCount = (int) Math.ceil((double) contentSize / chunkSize);
            if (chunkCount <= MAX_PREFERRED_CHUNKS) {
                break;
            }
            chunkSize *= 2; // Next power of 2
        }

        return chunkSize;
    }

    private static long validateChunkSize(long chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive: " + chunkSize);
        }
        if (Long.bitCount(chunkSize) != 1) {
            throw new IllegalArgumentException("Chunk size must be a power of 2: " + chunkSize);
        }
        return chunkSize;
    }

    private static long validateContentSize(long contentSize) {
        if (contentSize < 0) {
            throw new IllegalArgumentException("Content size cannot be negative: " + contentSize);
        }
        return contentSize;
    }

    private static int calculateTotalChunks(long contentSize, long chunkSize) {
        if (contentSize == 0) {
            return 0;
        }
        return (int) Math.ceil((double) contentSize / chunkSize);
    }

    /**
     * Calculates the capacity of leaf nodes (next power of 2).
     * @return The capacity of leaf nodes
     */
    private int calculateCapLeaf() {
        int capLeaf = 1;
        while (capLeaf < totalChunks) {
            capLeaf <<= 1;
        }
        return capLeaf;
    }

    /**
     * Calculates the total number of nodes in the Merkle tree.
     * @param capLeaf The capacity of leaf nodes
     * @return The total number of nodes
     */
    private int calculateNodeCount(int capLeaf) {
        return 2 * capLeaf - 1;
    }

    /**
     * Calculates the offset of the first leaf node.
     * @param capLeaf The capacity of leaf nodes
     * @return The offset of the first leaf node
     */
    private int calculateOffset(int capLeaf) {
        return capLeaf - 1;
    }

    /**
     * Calculates the number of internal nodes in the Merkle tree.
     * @param nodeCount The total number of nodes
     * @param leafCount The number of leaf nodes
     * @return The number of internal nodes
     */
    private int calculateInternalNodeCount(int nodeCount, int leafCount) {
        return nodeCount - leafCount;
    }

    /**
     * Creates a ChunkGeometryDescriptor with a specific chunk size, bypassing the automatic calculation.
     * This is primarily for testing purposes where specific chunk sizes are needed.
     * 
     * @param totalContentSize The total content size in bytes
     * @param chunkSize The specific chunk size to use
     * @return A ChunkGeometryDescriptor with the specified chunk size
     */
    public static ChunkGeometryDescriptor withSpecificChunkSize(long totalContentSize, long chunkSize) {
        return new ChunkGeometryDescriptor(totalContentSize, chunkSize);
    }

    // Getters for all properties

    /**
     * Gets the chunk size in bytes.
     * 
     * @return The size of each chunk in bytes
     */
    public long getChunkSize() {
        return chunkSize;
    }

    /**
     * Gets the total content size in bytes.
     * 
     * @return The total size of the content in bytes
     */
    public long getTotalContentSize() {
        return totalContentSize;
    }

    /**
     * Gets the total number of chunks.
     * 
     * @return The total number of chunks required to represent the content
     */
    public int getTotalChunks() {
        return totalChunks;
    }

    /**
     * Gets the number of leaf nodes in the merkle tree.
     * 
     * @return The number of leaf nodes
     */
    public int getLeafCount() {
        return leafCount;
    }

    /**
     * Gets the capacity for leaf nodes (next power of 2 >= leafCount).
     * 
     * @return The leaf node capacity
     */
    public int getCapLeaf() {
        return capLeaf;
    }

    /**
     * Gets the total number of nodes in the merkle tree.
     * 
     * @return The total node count (internal nodes + leaf nodes)
     */
    public int getNodeCount() {
        return nodeCount;
    }

    /**
     * Gets the offset where leaf nodes start in the merkle tree array.
     * 
     * @return The leaf node offset
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Gets the number of internal (non-leaf) nodes in the merkle tree.
     * 
     * @return The internal node count
     */
    public int getInternalNodeCount() {
        return internalNodeCount;
    }

    /**
     * Legacy method for compatibility with existing code.
     * 
     * @return The total content size in bytes
     * @deprecated Use getTotalContentSize() instead
     */
    @Deprecated
    public long getTotalFileSize() {
        return totalContentSize;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        ChunkGeometryDescriptor that = (ChunkGeometryDescriptor) obj;
        return chunkSize == that.chunkSize && 
               totalContentSize == that.totalContentSize && 
               totalChunks == that.totalChunks &&
               leafCount == that.leafCount &&
               capLeaf == that.capLeaf &&
               nodeCount == that.nodeCount &&
               offset == that.offset &&
               internalNodeCount == that.internalNodeCount;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(chunkSize);
        result = 31 * result + Long.hashCode(totalContentSize);
        result = 31 * result + Integer.hashCode(totalChunks);
        result = 31 * result + Integer.hashCode(leafCount);
        result = 31 * result + Integer.hashCode(capLeaf);
        result = 31 * result + Integer.hashCode(nodeCount);
        result = 31 * result + Integer.hashCode(offset);
        result = 31 * result + Integer.hashCode(internalNodeCount);
        return result;
    }

    @Override
    public String toString() {
        return "ChunkGeometryDescriptor{" +
               "chunkSize=" + chunkSize + " (" + (chunkSize / (1024 * 1024)) + "MB)" +
               ", totalContentSize=" + totalContentSize +
               ", totalChunks=" + totalChunks +
               ", leafCount=" + leafCount +
               ", capLeaf=" + capLeaf +
               ", nodeCount=" + nodeCount +
               ", offset=" + offset +
               ", internalNodeCount=" + internalNodeCount +
               '}';
    }
}
