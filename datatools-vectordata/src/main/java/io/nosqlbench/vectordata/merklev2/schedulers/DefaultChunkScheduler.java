package io.nosqlbench.vectordata.merklev2.schedulers;

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

import io.nosqlbench.vectordata.merklev2.ChunkScheduler;
import io.nosqlbench.vectordata.merklev2.MerkleShape;
import io.nosqlbench.vectordata.merklev2.MerkleState;
import io.nosqlbench.vectordata.merklev2.SchedulingTarget;
import io.nosqlbench.vectordata.merklev2.SchedulingDecision;
import io.nosqlbench.vectordata.merklev2.SchedulingReason;
import io.nosqlbench.vectordata.merklev2.NodeCandidate;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

/// Default stateless implementation of ChunkScheduler that provides balanced download scheduling.
/// 
/// This implementation provides a balanced approach between aggressive and conservative:
/// - Uses internal nodes when they provide reasonable efficiency (>= 60%)
/// - Includes moderate prefetching for sequential patterns
/// - Prioritizes exact coverage over excessive over-downloading
/// - Balances bandwidth utilization with download precision
/// 
/// Strategy:
/// - Prefer internal nodes for 3+ contiguous chunks
/// - Use moderate prefetching (1 chunk before/after for sequential patterns)
/// - Accept up to 40% over-downloading if it consolidates requests
/// - Fall back to leaf nodes for sparse or inefficient patterns
/// 
/// Best suited for:
/// - General purpose workloads
/// - Mixed access patterns
/// - Moderate bandwidth connections
/// - Balanced efficiency and performance requirements
public class DefaultChunkScheduler implements ChunkScheduler {
    
    ///
    /// Minimum efficiency ratio for internal nodes in default scheduler.
    /// Internal nodes with efficiency below this threshold will be rejected
    /// in favor of more targeted leaf node downloads.
    ///
    private static final double MIN_INTERNAL_NODE_EFFICIENCY = 0.6;
    
    ///
    /// Minimum chunks per internal node to justify consolidation.
    /// Internal nodes covering fewer chunks provide limited benefit.
    ///
    private static final int MIN_CHUNKS_FOR_CONSOLIDATION = 3;
    
    @Override
    public List<SchedulingDecision> selectOptimalNodes(
            List<Integer> requiredChunks,
            MerkleShape shape,
            MerkleState state) {
        
        if (requiredChunks.isEmpty()) {
            return List.of();
        }
        
        List<SchedulingDecision> decisions = new ArrayList<>();
        Set<Integer> remainingChunks = new HashSet<>(requiredChunks);
        int priority = 0;
        
        // Add moderate prefetching for sequential patterns
        Set<Integer> expandedChunks = new HashSet<>(requiredChunks);
        addModeratePrefetch(requiredChunks, expandedChunks, shape, state);
        
        // Try to find efficient internal nodes first (balanced approach)
        for (int level = 0; level < 8; level++) { // Moderate depth limit
            if (remainingChunks.isEmpty()) break;
            
            List<NodeCandidate> levelCandidates = findBalancedNodeCandidatesAtLevel(
                shape, state, expandedChunks, remainingChunks, level);
            
            // Sort by a balanced criteria: coverage first, then efficiency
            levelCandidates.sort((a, b) -> {
                int coverageCompare = Integer.compare(b.getRequiredChunkCount(), 
                                                     a.getRequiredChunkCount());
                if (coverageCompare != 0) return coverageCompare;
                return Double.compare(b.efficiency(), a.efficiency());
            });
            
            // Select candidates that meet balanced criteria
            for (NodeCandidate candidate : levelCandidates) {
                if (remainingChunks.isEmpty()) break;
                
                if (candidate.coveredRequiredChunks().stream().anyMatch(remainingChunks::contains)) {
                    SchedulingReason reason = determineBalancedSchedulingReason(candidate, requiredChunks);
                    
                    SchedulingDecision decision = new SchedulingDecision(
                        candidate.nodeIndex(),
                        reason,
                        priority++,
                        candidate.estimatedBytes(),
                        List.copyOf(candidate.coveredRequiredChunks()),
                        List.copyOf(candidate.coveredAllChunks()),
                        candidate.explanation()
                    );
                    
                    decisions.add(decision);
                    remainingChunks.removeAll(candidate.coveredRequiredChunks());
                }
            }
        }
        
        // Fall back to leaf nodes for any remaining chunks
        for (Integer chunkIndex : remainingChunks) {
            int leafNodeIndex = shape.chunkIndexToLeafNode(chunkIndex);
            MerkleShape.MerkleNodeRange byteRange = shape.getByteRangeForNode(leafNodeIndex);
            
            SchedulingDecision decision = new SchedulingDecision(
                leafNodeIndex,
                SchedulingReason.MINIMAL_DOWNLOAD,
                priority++,
                byteRange.getLength(),
                List.of(chunkIndex),
                List.of(chunkIndex),
                "Minimal leaf node download for chunk " + chunkIndex
            );
            
            decisions.add(decision);
        }
        
        return decisions;
    }
    
    @Override
    public void scheduleDownloads(
        long offset,
        long length,
        MerkleShape shape, MerkleState state,
        SchedulingTarget schedulingTarget) {
        // Get the optimal nodes for the requested byte range
        // This will prefer larger internal nodes when possible
        List<Integer> nodeIndices = shape.getNodesForByteRange(offset, length);
        
        // Create tasks for nodes that are not yet valid
        for (int nodeIndex : nodeIndices) {
            if (needsDownload(shape, state, nodeIndex)) {
                // Get byte range covered by this node
                MerkleShape.MerkleNodeRange byteRange = shape.getByteRangeForNode(nodeIndex);
                MerkleShape.MerkleNodeRange leafRange = shape.getLeafRangeForNode(nodeIndex);
                boolean isLeaf = shape.isLeafNode(nodeIndex);
                
                // Get or create a future for this node from the SchedulingTarget
                CompletableFuture<Void> future = schedulingTarget.getOrCreateFuture(nodeIndex);
                
                // Create the download task
                NodeDownloadTask task = new DefaultNodeDownloadTask(nodeIndex, byteRange.getStart(), 
                    byteRange.getLength(), isLeaf, leafRange, future);
                
                schedulingTarget.offerTask(task);
            }
        }
    }
    
    ///
    /// Adds moderate prefetching to the expanded chunk set.
    /// Default scheduler uses conservative prefetching: 1 chunk before/after for sequential patterns.
    ///
    /// @param requiredChunks The original required chunks
    /// @param expandedChunks The set to add prefetch chunks to
    /// @param shape The merkle shape
    /// @param state The merkle state
    ///
    private void addModeratePrefetch(List<Integer> requiredChunks, Set<Integer> expandedChunks, 
                                   MerkleShape shape, MerkleState state) {
        // Only add prefetch for sequential patterns (3+ contiguous chunks)
        if (requiredChunks.size() < 3) return;
        
        // Check if chunks are mostly sequential
        List<Integer> sortedChunks = new ArrayList<>(requiredChunks);
        sortedChunks.sort(Integer::compareTo);
        
        int sequentialCount = 0;
        for (int i = 1; i < sortedChunks.size(); i++) {
            if (sortedChunks.get(i) - sortedChunks.get(i-1) == 1) {
                sequentialCount++;
            }
        }
        
        // If mostly sequential (70%+ adjacent), add moderate prefetch
        if (sequentialCount >= (sortedChunks.size() - 1) * 0.7) {
            int minChunk = sortedChunks.get(0);
            int maxChunk = sortedChunks.get(sortedChunks.size() - 1);
            
            // Add 1 chunk before and after
            if (minChunk > 0 && !state.isValid(minChunk - 1)) {
                expandedChunks.add(minChunk - 1);
            }
            if (maxChunk < shape.getTotalChunks() - 1 && !state.isValid(maxChunk + 1)) {
                expandedChunks.add(maxChunk + 1);
            }
        }
    }
    
    ///
    /// Finds node candidates at a specific tree level with balanced criteria.
    ///
    /// @param shape The merkle shape
    /// @param state The merkle state
    /// @param expandedChunks All chunks being considered (including prefetch)
    /// @param remainingChunks Chunks that still need to be covered
    /// @param level Tree level to search (0 = root)
    /// @return List of node candidates at the specified level
    ///
    private List<NodeCandidate> findBalancedNodeCandidatesAtLevel(
            MerkleShape shape, MerkleState state, Set<Integer> expandedChunks,
            Set<Integer> remainingChunks, int level) {
        
        List<NodeCandidate> candidates = new ArrayList<>();
        List<Integer> nodesAtLevel = getInternalNodesAtLevel(shape, level);
        
        for (int nodeIndex : nodesAtLevel) {
            NodeCandidate candidate = evaluateNodeCandidate(
                shape, state, nodeIndex, expandedChunks, remainingChunks);
            
            if (candidate != null && 
                candidate.getRequiredChunkCount() >= MIN_CHUNKS_FOR_CONSOLIDATION &&
                candidate.efficiency() >= MIN_INTERNAL_NODE_EFFICIENCY) {
                candidates.add(candidate);
            }
        }
        
        return candidates;
    }
    
    ///
    /// Gets all internal nodes at a specific tree level.
    ///
    /// @param shape The merkle shape
    /// @param level Tree level (0 = root)
    /// @return List of node indices at the specified level
    ///
    private List<Integer> getInternalNodesAtLevel(MerkleShape shape, int level) {
        List<Integer> nodes = new ArrayList<>();
        
        if (level == 0) {
            // Root node
            if (shape.getInternalNodeCount() > 0) {
                nodes.add(0);
            }
            return nodes;
        }
        
        // Calculate node range for this level
        int levelStart = (1 << level) - 1; // 2^level - 1
        int levelSize = 1 << level; // 2^level
        int levelEnd = levelStart + levelSize;
        
        for (int i = levelStart; i < Math.min(levelEnd, shape.getInternalNodeCount()); i++) {
            nodes.add(i);
        }
        
        return nodes;
    }
    
    ///
    /// Evaluates a specific node as a scheduling candidate.
    ///
    /// @param shape The merkle shape
    /// @param state The merkle state
    /// @param nodeIndex The node to evaluate
    /// @param expandedChunks All chunks being considered
    /// @param remainingChunks Chunks that still need coverage
    /// @return NodeCandidate if viable, null otherwise
    ///
    private NodeCandidate evaluateNodeCandidate(
            MerkleShape shape, MerkleState state, int nodeIndex,
            Set<Integer> expandedChunks, Set<Integer> remainingChunks) {
        
        boolean isLeaf = shape.isLeafNode(nodeIndex);
        MerkleShape.MerkleNodeRange leafRange = shape.getLeafRangeForNode(nodeIndex);
        MerkleShape.MerkleNodeRange byteRange = shape.getByteRangeForNode(nodeIndex);
        
        // Find which chunks this node covers
        Set<Integer> coveredAllChunks = new HashSet<>();
        Set<Integer> coveredRequiredChunks = new HashSet<>();
        
        for (long leafIndex = leafRange.getStart(); leafIndex < leafRange.getEnd(); leafIndex++) {
            int chunkIndex = (int) leafIndex;
            if (chunkIndex < shape.getTotalChunks() && !state.isValid(chunkIndex)) {
                coveredAllChunks.add(chunkIndex);
                if (remainingChunks.contains(chunkIndex)) {
                    coveredRequiredChunks.add(chunkIndex);
                }
            }
        }
        
        // Skip nodes that don't cover any required chunks
        if (coveredRequiredChunks.isEmpty()) {
            return null;
        }
        
        // Calculate efficiency
        double efficiency = coveredAllChunks.isEmpty() ? 0.0 : 
            (double) coveredRequiredChunks.size() / coveredAllChunks.size();
        
        // Create explanation
        String explanation = String.format(
            "%s node %d covering %d required + %d prefetch chunks (efficiency %.2f, balanced approach)",
            isLeaf ? "Leaf" : "Internal", nodeIndex, 
            coveredRequiredChunks.size(), 
            coveredAllChunks.size() - coveredRequiredChunks.size(),
            efficiency
        );
        
        return new NodeCandidate(
            nodeIndex,
            isLeaf,
            coveredRequiredChunks,
            coveredAllChunks,
            byteRange.getLength(),
            efficiency,
            explanation
        );
    }
    
    ///
    /// Determines the scheduling reason for a node candidate using balanced criteria.
    ///
    /// @param candidate The node candidate
    /// @param originalRequired The original required chunks (before expansion)
    /// @return The appropriate scheduling reason
    ///
    private SchedulingReason determineBalancedSchedulingReason(NodeCandidate candidate, List<Integer> originalRequired) {
        if (candidate.efficiency() >= 0.95) {
            return SchedulingReason.EXACT_MATCH;
        } else if (candidate.efficiency() >= 0.8) {
            return SchedulingReason.EFFICIENT_COVERAGE;
        } else if (candidate.getRequiredChunkCount() >= MIN_CHUNKS_FOR_CONSOLIDATION) {
            return SchedulingReason.CONSOLIDATION;
        } else if (candidate.getExcessChunkCount() > 0) {
            return SchedulingReason.BANDWIDTH_OPTIMIZATION;
        } else {
            return SchedulingReason.MINIMAL_DOWNLOAD;
        }
    }
    
    /// Determines if a node needs to be downloaded based on the validity of its leaf nodes.
    /// 
    /// For leaf nodes, checks if the corresponding chunk is valid.
    /// For internal nodes, checks if all leaf nodes under it are valid.
    /// 
    /// @param shape The merkle shape for node analysis
    /// @param state The merkle state for validity checking
    /// @param nodeIndex The node to check
    /// @return true if the node needs to be downloaded
    private boolean needsDownload(MerkleShape shape, MerkleState state, int nodeIndex) {
        if (shape.isLeafNode(nodeIndex)) {
            // For leaf nodes, check the corresponding chunk
            int chunkIndex = shape.leafNodeToChunkIndex(nodeIndex);
            return !state.isValid(chunkIndex);
        } else {
            // For internal nodes, check all leaf nodes under it
            MerkleShape.MerkleNodeRange leafRange = shape.getLeafRangeForNode(nodeIndex);
            for (long leafIndex = leafRange.getStart(); leafIndex < leafRange.getEnd(); leafIndex++) {
                int chunkIndex = shape.leafNodeToChunkIndex((int) leafIndex);
                if (!state.isValid(chunkIndex)) {
                    return true;
                }
            }
            return false;
        }
    }
    
    
    
    /// Default implementation of NodeDownloadTask.
    private static class DefaultNodeDownloadTask implements NodeDownloadTask {
        private final int nodeIndex;
        private final long offset;
        private final long size;
        private final boolean isLeafNode;
        private final MerkleShape.MerkleNodeRange leafRange;
        private final CompletableFuture<Void> future;
        
        public DefaultNodeDownloadTask(int nodeIndex, long offset, long size, boolean isLeafNode, 
                                     MerkleShape.MerkleNodeRange leafRange, CompletableFuture<Void> future) {
            this.nodeIndex = nodeIndex;
            this.offset = offset;
            this.size = size;
            this.isLeafNode = isLeafNode;
            this.leafRange = leafRange;
            this.future = future;
        }
        
        @Override
        public int getNodeIndex() {
            return nodeIndex;
        }
        
        @Override
        public long getOffset() {
            return offset;
        }
        
        @Override
        public long getSize() {
            return size;
        }
        
        @Override
        public boolean isLeafNode() {
            return isLeafNode;
        }
        
        @Override
        public MerkleShape.MerkleNodeRange getLeafRange() {
            return leafRange;
        }
        
        @Override
        public CompletableFuture<Void> getFuture() {
            return future;
        }
        
        @Override
        public String toString() {
            return String.format("NodeDownloadTask{nodeIndex=%d, offset=%d, size=%d, isLeaf=%s, leafRange=[%d,%d)}", 
                nodeIndex, offset, size, isLeafNode, leafRange.getStart(), leafRange.getEnd());
        }
    }
}