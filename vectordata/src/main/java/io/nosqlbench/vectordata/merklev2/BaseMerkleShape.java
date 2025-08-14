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

import java.util.ArrayList;
import java.util.List;

// ChunkBoundary is now in the same package

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
public final class BaseMerkleShape implements MerkleShape {

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
     * Creates a new MerkleShape with automatically calculated chunk size.
     * The chunk size is determined to keep the number of chunks at or below 4096
     * while using power-of-2 sizes between 1MB and 64MB.
     * 
     * @param totalContentSize The total size of the content in bytes (must be non-negative)
     * @throws IllegalArgumentException if contentSize is negative
     */
    public BaseMerkleShape(long totalContentSize) {
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
     * Creates a new MerkleShape with a specific chunk size.
     * This is primarily for testing purposes where specific chunk sizes are needed.
     * 
     * @param totalContentSize The total content size in bytes
     * @param chunkSize The specific chunk size to use
     * @throws IllegalArgumentException if chunkSize is not positive
     */
    public BaseMerkleShape(long totalContentSize, long chunkSize) {
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
    private BaseMerkleShape(
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
     * Creates a MerkleShape for the specified total content size.
     * This is the single entry point for obtaining a descriptor based on content size.
     *
     * @param totalContentSize The total size of the content in bytes (must be non-negative)
     * @return A MerkleShape capturing all geometry and Merkle tree dimensions
     * @throws IllegalArgumentException if totalContentSize is negative
     */
    public static BaseMerkleShape fromContentSize(long totalContentSize) {
        return new BaseMerkleShape(totalContentSize);
    }

    /**
     * Gets the chunk boundary information for the specified chunk index.
     * This provides a comprehensive view of the chunk's position and size.
     * 
     * @param chunkIndex The index of the chunk (0-based)
     * @return ChunkBoundary containing start, end, and size information
     * @throws IllegalArgumentException if chunkIndex is out of bounds
     */
    @Override
    public ChunkBoundary getChunkBoundary(int chunkIndex) {
        validateChunkIndex(chunkIndex);

        long startInclusive = getChunkStartPosition(chunkIndex);
        long endExclusive = getChunkEndPosition(chunkIndex);

        return new ChunkBoundary(chunkIndex, startInclusive, endExclusive);
    }

    /**
     * Gets the starting position (inclusive) of the specified chunk.
     * This is the SINGLE AUTHORITATIVE calculation for chunk start positions.
     * 
     * @param chunkIndex The index of the chunk (0-based)
     * @return The starting byte position of the chunk (inclusive)
     * @throws IllegalArgumentException if chunkIndex is out of bounds
     */
    @Override
    public long getChunkStartPosition(int chunkIndex) {
        validateChunkIndex(chunkIndex);
        return (long) chunkIndex * chunkSize;
    }

    /**
     * Gets the ending position (exclusive) of the specified chunk.
     * This is the SINGLE AUTHORITATIVE calculation for chunk end positions.
     * 
     * @param chunkIndex The index of the chunk (0-based)
     * @return The ending byte position of the chunk (exclusive)
     * @throws IllegalArgumentException if chunkIndex is out of bounds
     */
    @Override
    public long getChunkEndPosition(int chunkIndex) {
        validateChunkIndex(chunkIndex);
        long startInclusive = (long) chunkIndex * chunkSize;
        return Math.min(startInclusive + chunkSize, totalContentSize);
    }

    /**
     * Gets the actual size of the specified chunk in bytes.
     * This is the SINGLE AUTHORITATIVE calculation for individual chunk sizes.
     * 
     * @param chunkIndex The index of the chunk (0-based)
     * @return The actual size of the chunk in bytes
     * @throws IllegalArgumentException if chunkIndex is out of bounds
     */
    @Override
    public long getActualChunkSize(int chunkIndex) {
        validateChunkIndex(chunkIndex);
        long startInclusive = (long) chunkIndex * chunkSize;
        long endExclusive = Math.min(startInclusive + chunkSize, totalContentSize);
        return endExclusive - startInclusive;
    }

    /**
     * Gets the chunk index that contains the specified content position.
     * This is the SINGLE AUTHORITATIVE calculation for position-to-chunk mapping.
     * 
     * @param contentPosition The position in the content (0-based)
     * @return The chunk index containing this position
     * @throws IllegalArgumentException if contentPosition is out of bounds
     */
    @Override
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
    @Override
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
    @Override
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
     * Calculates the optimal chunk size for the given content size.
     * 
     * Algorithm:
     * 1. For very small content sizes (< 1KB), use a smaller minimum chunk size
     * 2. For small content sizes (< 1MB), use a chunk size that's appropriate for the content
     * 3. For larger content sizes, start with minimum chunk size (1MB)
     * 4. While chunks would exceed 4096 and chunk size < 64MB, double the chunk size
     * 5. Use the resulting chunk size
     * 
     * @param contentSize The total content size
     * @return The optimal chunk size (power of 2)
     */
    private static long calculateOptimalChunkSize(long contentSize) {
        if (contentSize == 0) {
            return MIN_CHUNK_SIZE;
        }

        // For very small content sizes, use a smaller chunk size
        if (contentSize < 1024) { // Less than 1KB
            return 64; // Use 64 bytes as minimum for very small content
        }

        // For small content sizes, use a chunk size that's appropriate
        if (contentSize < MIN_CHUNK_SIZE) {
            // Find the next power of 2 that's greater than or equal to the content size
            long smallChunkSize = 1024; // Start with 1KB
            while (smallChunkSize < contentSize) {
                smallChunkSize *= 2;
            }
            return smallChunkSize;
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

    // Getters for all properties

    /**
     * Gets the chunk size in bytes.
     * 
     * @return The size of each chunk in bytes
     */
    @Override
    public long getChunkSize() {
        return chunkSize;
    }

    /**
     * Gets the total content size in bytes.
     * 
     * @return The total size of the content in bytes
     */
    @Override
    public long getTotalContentSize() {
        return totalContentSize;
    }

    /**
     * Gets the total number of chunks.
     * 
     * @return The total number of chunks required to represent the content
     */
    @Override
    public int getTotalChunks() {
        return totalChunks;
    }

    /**
     * Gets the number of leaf nodes in the merkle tree.
     * 
     * @return The number of leaf nodes
     */
    @Override
    public int getLeafCount() {
        return leafCount;
    }

    /**
     * Gets the capacity for leaf nodes (next power of 2 >= leafCount).
     * 
     * @return The leaf node capacity
     */
    @Override
    public int getCapLeaf() {
        return capLeaf;
    }

    /**
     * Gets the total number of nodes in the merkle tree.
     * 
     * @return The total node count (internal nodes + leaf nodes)
     */
    @Override
    public int getNodeCount() {
        return nodeCount;
    }

    /**
     * Gets the offset where leaf nodes start in the merkle tree array.
     * 
     * @return The leaf node offset
     */
    @Override
    public int getOffset() {
        return offset;
    }

    /**
     * Gets the number of internal (non-leaf) nodes in the merkle tree.
     * 
     * @return The internal node count
     */
    @Override
    public int getInternalNodeCount() {
        return internalNodeCount;
    }

    @Override
    public MerkleNodeRange getLeafRangeForNode(int nodeIndex) {
        if (nodeIndex < 0 || nodeIndex >= nodeCount) {
            throw new IllegalArgumentException("Node index " + nodeIndex + " out of bounds [0, " + nodeCount + ")");
        }
        
        if (nodeIndex >= offset) {
            // This is a leaf node
            int leafIndex = nodeIndex - offset;
            return new SimpleMerkleNodeRange(leafIndex, leafIndex + 1);
        } else {
            // This is an internal node - calculate range of leaves it covers
            int level = getNodeLevel(nodeIndex);
            int nodesPerLevel = 1 << level;
            int nodePositionInLevel = nodeIndex - ((1 << level) - 1);
            int leavesPerNode = capLeaf / nodesPerLevel;
            
            long startLeaf = (long) nodePositionInLevel * leavesPerNode;
            long endLeaf = Math.min(startLeaf + leavesPerNode, leafCount);
            
            return new SimpleMerkleNodeRange(startLeaf, endLeaf);
        }
    }
    
    @Override
    public MerkleNodeRange getByteRangeForNode(int nodeIndex) {
        MerkleNodeRange leafRange = getLeafRangeForNode(nodeIndex);
        
        // Convert leaf range to chunk indices, ensuring bounds
        int startChunk = (int) leafRange.getStart();
        int endChunk = (int) Math.min(leafRange.getEnd(), totalChunks);
        
        // Handle case where start chunk is beyond bounds
        if (startChunk >= totalChunks) {
            return new SimpleMerkleNodeRange(totalContentSize, totalContentSize);
        }
        
        // Convert chunk indices to byte range
        long startByte = getChunkStartPosition(startChunk);
        long endByte = (endChunk >= totalChunks) ? totalContentSize : getChunkStartPosition(endChunk);
        
        return new SimpleMerkleNodeRange(startByte, endByte);
    }
    
    @Override
    public java.util.List<Integer> getNodesForByteRange(long startByte, long length) {
        validateContentPosition(startByte);
        if (length <= 0) {
            throw new IllegalArgumentException("Length must be positive: " + length);
        }
        
        java.util.List<Integer> nodes = new java.util.ArrayList<>();
        long endByte = Math.min(startByte + length, totalContentSize);
        
        // Find the range of chunks
        int startChunk = getChunkIndexForPosition(startByte);
        int endChunk = getChunkIndexForPosition(endByte - 1);
        
        // Try to find larger internal nodes that cover multiple chunks
        findOptimalNodes(startChunk, endChunk, nodes);
        
        return nodes;
    }
    
    @Override
    public boolean isLeafNode(int nodeIndex) {
        if (nodeIndex < 0 || nodeIndex >= nodeCount) {
            throw new IllegalArgumentException("Node index " + nodeIndex + " out of bounds [0, " + nodeCount + ")");
        }
        return nodeIndex >= offset;
    }
    
    @Override
    public int chunkIndexToLeafNode(int chunkIndex) {
        validateChunkIndex(chunkIndex);
        return offset + chunkIndex;
    }
    
    @Override
    public int leafNodeToChunkIndex(int leafNodeIndex) {
        if (leafNodeIndex < offset || leafNodeIndex >= nodeCount) {
            throw new IllegalArgumentException("Not a valid leaf node index: " + leafNodeIndex);
        }
        return leafNodeIndex - offset;
    }
    
    private int getNodeLevel(int nodeIndex) {
        // Calculate the level of a node in the tree (0 = root)
        if (nodeIndex >= offset) {
            // Leaf nodes are at the deepest level
            return Integer.numberOfTrailingZeros(capLeaf);
        }
        
        // For internal nodes, calculate based on position
        int level = 0;
        int levelStart = 0;
        int levelSize = 1;
        
        while (levelStart + levelSize <= nodeIndex) {
            levelStart += levelSize;
            levelSize *= 2;
            level++;
        }
        
        return level;
    }
    
    private void findOptimalNodes(int startChunk, int endChunk, java.util.List<Integer> nodes) {
        // Simple implementation: just use leaf nodes for now
        // TODO: Optimize to use internal nodes when they cover complete ranges
        for (int chunk = startChunk; chunk <= endChunk; chunk++) {
            nodes.add(chunkIndexToLeafNode(chunk));
        }
    }
    
    /// Simple implementation of MerkleNodeRange
    private static class SimpleMerkleNodeRange implements MerkleNodeRange {
        private final long start;
        private final long end;
        
        public SimpleMerkleNodeRange(long start, long end) {
            this.start = start;
            this.end = end;
        }
        
        @Override
        public long getStart() {
            return start;
        }
        
        @Override
        public long getEnd() {
            return end;
        }
        
        @Override
        public long getLength() {
            return end - start;
        }
        
        @Override
        public boolean contains(long position) {
            return position >= start && position < end;
        }
        
        @Override
        public boolean overlaps(MerkleNodeRange other) {
            return start < other.getEnd() && end > other.getStart();
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        BaseMerkleShape that = (BaseMerkleShape) obj;
        return chunkSize == that.chunkSize && 
               totalContentSize == that.totalContentSize && 
               totalChunks == that.totalChunks &&
               leafCount == that.leafCount &&
               capLeaf == that.capLeaf &&
               nodeCount == that.nodeCount &&
               offset == that.offset &&
               internalNodeCount == that.internalNodeCount;
    }

    private void validateNodeIndex(int nodeIndex) {
        if (nodeIndex < 0 || nodeIndex >= nodeCount) {
            throw new IllegalArgumentException(
                "Node index out of bounds: " + nodeIndex + " (valid range: 0 to " + (nodeCount - 1) + ")");
        }
    }
    
    @Override
    public List<Integer> getChunksForNode(int nodeIndex) {
        validateNodeIndex(nodeIndex);
        List<Integer> chunks = new ArrayList<>();
        
        if (isLeafNode(nodeIndex)) {
            // For leaf nodes, return only the corresponding chunk
            int chunkIndex = leafNodeToChunkIndex(nodeIndex);
            if (chunkIndex < totalChunks) {
                chunks.add(chunkIndex);
            }
        } else {
            // For internal nodes, get all chunks covered by leaf range
            MerkleNodeRange leafRange = getLeafRangeForNode(nodeIndex);
            for (long leafIdx = leafRange.getStart(); leafIdx < leafRange.getEnd() && leafIdx < totalChunks; leafIdx++) {
                chunks.add((int) leafIdx);
            }
        }
        
        return chunks;
    }
    
    @Override
    public List<Integer> getInternalNodesAtLevel(int level) {
        List<Integer> nodes = new ArrayList<>();
        
        if (level < 0) {
            throw new IllegalArgumentException("Level must be non-negative, got: " + level);
        }
        
        if (level == 0) {
            // Root node
            if (internalNodeCount > 0) {
                nodes.add(0);
            }
            return nodes;
        }
        
        // Calculate node range for this level
        // In a binary tree, level L has nodes from 2^L - 1 to 2^(L+1) - 2
        int levelStart = (1 << level) - 1; // 2^level - 1
        int levelSize = 1 << level; // 2^level
        int levelEnd = levelStart + levelSize;
        
        // Only add nodes that exist in our tree
        for (int i = levelStart; i < Math.min(levelEnd, internalNodeCount); i++) {
            nodes.add(i);
        }
        
        return nodes;
    }
    
    @Override
    public boolean nodeHasInvalidChunks(int nodeIndex, MerkleState state) {
        validateNodeIndex(nodeIndex);
        
        // Get all chunks covered by this node
        List<Integer> chunks = getChunksForNode(nodeIndex);
        
        // Check if any chunk is invalid
        for (Integer chunkIndex : chunks) {
            if (!state.isValid(chunkIndex)) {
                return true;
            }
        }
        
        return false;
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
        return "MerkleShape{" +
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
