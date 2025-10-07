package io.nosqlbench.vectordata.simulation.mockdriven;

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

import io.nosqlbench.vectordata.merklev2.ChunkBoundary;
import io.nosqlbench.vectordata.merklev2.MerkleShape;
import io.nosqlbench.vectordata.merklev2.MerkleState;

import java.util.ArrayList;
import java.util.List;

/// Simplified implementation of MerkleShape for simulation testing.
/// 
/// This implementation provides a lightweight merkle tree structure suitable
/// for testing scheduler algorithms without the complexity of the full
/// production implementation. It focuses on the essential tree geometry
/// calculations needed for scheduler evaluation.
/// 
/// Key features:
/// - Configurable chunk size and content size
/// - Simplified tree structure calculations
/// - Node-to-byte-range mapping for scheduler testing
/// - Minimal memory footprint for large-scale simulations
/// 
/// Usage:
/// ```java
/// SimulatedMerkleShape shape = new SimulatedMerkleShape(
///     1_000_000_000L, // 1GB file
///     1024 * 1024     // 1MB chunks
/// );
/// 
/// // Use with schedulers
/// List<Integer> nodes = shape.getNodesForByteRange(0, 4096);
/// ```
public class SimulatedMerkleShape implements MerkleShape {
    
    private final long totalContentSize;
    private final long chunkSize;
    private final int totalChunks;
    private final int leafCount;
    private final int capLeaf;
    private final int nodeCount;
    private final int offset;
    private final int internalNodeCount;
    
    /// Creates a simulated merkle shape with specified parameters.
    /// 
    /// @param totalContentSize Total size of the content in bytes
    /// @param chunkSize Size of each chunk in bytes
    public SimulatedMerkleShape(long totalContentSize, long chunkSize) {
        if (totalContentSize < 0) {
            throw new IllegalArgumentException("Content size cannot be negative");
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }
        
        this.totalContentSize = totalContentSize;
        this.chunkSize = chunkSize;
        this.totalChunks = (int) Math.ceil((double) totalContentSize / chunkSize);
        this.leafCount = totalChunks;
        
        // Calculate capacity as next power of 2
        this.capLeaf = nextPowerOf2(leafCount);
        
        // In a balanced binary tree, internal nodes = capacity - 1
        this.internalNodeCount = capLeaf - 1;
        this.offset = internalNodeCount;
        this.nodeCount = internalNodeCount + capLeaf;
    }
    
    @Override
    public long getChunkStartPosition(int chunkIndex) {
        validateChunkIndex(chunkIndex);
        return (long) chunkIndex * chunkSize;
    }
    
    @Override
    public long getChunkEndPosition(int chunkIndex) {
        validateChunkIndex(chunkIndex);
        long endPos = (long) (chunkIndex + 1) * chunkSize;
        return Math.min(endPos, totalContentSize);
    }
    
    @Override
    public long getActualChunkSize(int chunkIndex) {
        validateChunkIndex(chunkIndex);
        return getChunkEndPosition(chunkIndex) - getChunkStartPosition(chunkIndex);
    }
    
    @Override
    public int getChunkIndexForPosition(long contentPosition) {
        validateContentPosition(contentPosition);
        return (int) (contentPosition / chunkSize);
    }
    
    @Override
    public void validateChunkIndex(int chunkIndex) {
        if (chunkIndex < 0 || chunkIndex >= totalChunks) {
            throw new IllegalArgumentException("Chunk index out of bounds: " + chunkIndex + 
                                             " (valid range: 0 to " + (totalChunks - 1) + ")");
        }
    }
    
    @Override
    public void validateContentPosition(long contentPosition) {
        if (contentPosition < 0 || contentPosition >= totalContentSize) {
            throw new IllegalArgumentException("Content position out of bounds: " + contentPosition + 
                                             " (valid range: 0 to " + (totalContentSize - 1) + ")");
        }
    }
    
    @Override
    public long getChunkSize() {
        return chunkSize;
    }
    
    @Override
    public long getTotalContentSize() {
        return totalContentSize;
    }
    
    @Override
    public int getTotalChunks() {
        return totalChunks;
    }
    
    @Override
    public int getLeafCount() {
        return leafCount;
    }
    
    @Override
    public int getCapLeaf() {
        return capLeaf;
    }
    
    @Override
    public int getNodeCount() {
        return nodeCount;
    }
    
    @Override
    public int getOffset() {
        return offset;
    }
    
    @Override
    public int getInternalNodeCount() {
        return internalNodeCount;
    }
    
    @Override
    public ChunkBoundary getChunkBoundary(int chunkIndex) {
        validateChunkIndex(chunkIndex);
        return new ChunkBoundary(
            chunkIndex,
            getChunkStartPosition(chunkIndex),
            getChunkEndPosition(chunkIndex)
        );
    }
    
    @Override
    public MerkleNodeRange getLeafRangeForNode(int nodeIndex) {
        validateNodeIndex(nodeIndex);
        
        if (nodeIndex >= offset) {
            // This is a leaf node
            int leafIndex = nodeIndex - offset;
            if (leafIndex >= leafCount) {
                // This is an unused leaf in the padded capacity
                return new SimpleRange(leafIndex, leafIndex);
            }
            return new SimpleRange(leafIndex, leafIndex + 1);
        } else {
            // This is an internal node - calculate range of leaves it covers
            int level = getNodeLevel(nodeIndex);
            int nodePositionInLevel = getNodePositionInLevel(nodeIndex, level);
            int leavesPerNode = capLeaf >> level; // capLeaf / (2^level)
            
            long startLeaf = (long) nodePositionInLevel * leavesPerNode;
            long endLeaf = Math.min(startLeaf + leavesPerNode, leafCount);
            
            return new SimpleRange(startLeaf, endLeaf);
        }
    }
    
    @Override
    public MerkleNodeRange getByteRangeForNode(int nodeIndex) {
        MerkleNodeRange leafRange = getLeafRangeForNode(nodeIndex);
        
        if (leafRange.getLength() == 0) {
            return new SimpleRange(0, 0);
        }
        
        long startByte = getChunkStartPosition((int) leafRange.getStart());
        long endByte = leafRange.getEnd() > leafCount ? totalContentSize : 
                      getChunkEndPosition((int) (leafRange.getEnd() - 1));
        
        return new SimpleRange(startByte, endByte);
    }
    
    @Override
    public List<Integer> getNodesForByteRange(long startByte, long length) {
        if (startByte < 0 || length <= 0) {
            throw new IllegalArgumentException("Invalid byte range");
        }
        if (startByte >= totalContentSize) {
            return new ArrayList<>();
        }
        
        long endByte = Math.min(startByte + length, totalContentSize);
        int startChunk = getChunkIndexForPosition(startByte);
        int endChunk = getChunkIndexForPosition(endByte - 1);
        
        List<Integer> nodes = new ArrayList<>();
        
        // For simulation, use simple approach: return leaf nodes for all affected chunks
        for (int chunk = startChunk; chunk <= endChunk; chunk++) {
            nodes.add(chunkIndexToLeafNode(chunk));
        }
        
        return nodes;
    }
    
    @Override
    public boolean isLeafNode(int nodeIndex) {
        validateNodeIndex(nodeIndex);
        return nodeIndex >= offset;
    }
    
    @Override
    public int chunkIndexToLeafNode(int chunkIndex) {
        validateChunkIndex(chunkIndex);
        return offset + chunkIndex;
    }
    
    @Override
    public int leafNodeToChunkIndex(int leafNodeIndex) {
        if (!isLeafNode(leafNodeIndex)) {
            throw new IllegalArgumentException("Node " + leafNodeIndex + " is not a leaf node");
        }
        int chunkIndex = leafNodeIndex - offset;
        if (chunkIndex >= totalChunks) {
            throw new IllegalArgumentException("Leaf node " + leafNodeIndex + " does not correspond to a valid chunk");
        }
        return chunkIndex;
    }
    
    /// Validates that a node index is within bounds.
    private void validateNodeIndex(int nodeIndex) {
        if (nodeIndex < 0 || nodeIndex >= nodeCount) {
            throw new IllegalArgumentException("Node index out of bounds: " + nodeIndex + 
                                             " (valid range: 0 to " + (nodeCount - 1) + ")");
        }
    }
    
    /// Gets the level of a node in the tree (0 = root level).
    private int getNodeLevel(int nodeIndex) {
        if (nodeIndex >= offset) {
            return Integer.numberOfTrailingZeros(capLeaf); // Leaf level
        }
        
        // For internal nodes, determine level by position
        int remaining = nodeIndex + 1;
        int level = 0;
        int levelSize = 1;
        
        while (remaining > levelSize) {
            remaining -= levelSize;
            levelSize *= 2;
            level++;
        }
        
        return level;
    }
    
    /// Gets the position of a node within its level.
    private int getNodePositionInLevel(int nodeIndex, int level) {
        if (level == 0) {
            return 0; // Root node
        }
        
        int nodesBeforeLevel = (1 << level) - 1; // 2^level - 1
        return nodeIndex - nodesBeforeLevel;
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
    
    /// Calculates the next power of 2 greater than or equal to the given value.
    private int nextPowerOf2(int n) {
        if (n <= 1) return 1;
        return 1 << (32 - Integer.numberOfLeadingZeros(n - 1));
    }
    
    /// Simple implementation of MerkleNodeRange.
    private static class SimpleRange implements MerkleNodeRange {
        private final long start;
        private final long end;
        
        public SimpleRange(long start, long end) {
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
        
        @Override
        public String toString() {
            return String.format("[%d, %d)", start, end);
        }
    }
}