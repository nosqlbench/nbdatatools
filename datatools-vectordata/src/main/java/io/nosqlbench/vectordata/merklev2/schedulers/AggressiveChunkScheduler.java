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

/// Aggressive chunk scheduler that prioritizes downloading larger regions.
/// 
/// This scheduler attempts to maximize bandwidth utilization by preferring
/// to download entire internal nodes (covering multiple chunks) rather than
/// individual leaf nodes. It's optimized for high-bandwidth, low-latency
/// connections where the overhead of multiple small requests is significant.
/// 
/// Strategy:
/// - Always prefer the largest possible internal nodes
/// - Aggressively prefetch adjacent regions
/// - Minimize the total number of download operations
/// - Accept some over-downloading to reduce request overhead
/// 
/// Best suited for:
/// - High-bandwidth connections (100+ Mbps)
/// - Low-latency networks (< 50ms)
/// - Sequential or clustered access patterns
/// - Scenarios where bandwidth is more abundant than request overhead
public class AggressiveChunkScheduler implements ChunkScheduler {
    
    ///
    /// Maximum acceptable efficiency ratio for internal nodes.
    /// Internal nodes with efficiency below this threshold will be rejected
    /// in favor of leaf nodes, even in aggressive mode.
    ///
    private static final double MIN_INTERNAL_NODE_EFFICIENCY = 0.3;
    
    ///
    /// Preferred minimum chunks per internal node to justify the overhead.
    /// Single-chunk internal nodes provide no bandwidth advantage.
    ///
    private static final int MIN_CHUNKS_FOR_INTERNAL_NODE = 2;
    
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
        
        // Aggressively expand required chunks by adding adjacent invalid chunks for prefetching
        Set<Integer> expandedChunks = new HashSet<>(requiredChunks);
        for (Integer chunk : requiredChunks) {
            // Add up to 2 chunks before and 4 chunks after for aggressive prefetching
            for (int offset = -2; offset <= 4; offset++) {
                int candidateChunk = chunk + offset;
                if (candidateChunk >= 0 && candidateChunk < shape.getTotalChunks() && 
                    !state.isValid(candidateChunk)) {
                    expandedChunks.add(candidateChunk);
                }
            }
        }
        
        // Try to find large internal nodes that cover multiple expanded chunks efficiently
        for (int level = 0; level < 10; level++) { // Reasonable depth limit
            if (remainingChunks.isEmpty()) break;
            
            List<NodeCandidate> levelCandidates = findInternalNodeCandidatesAtLevel(
                shape, state, expandedChunks, remainingChunks, level);
            
            // Sort candidates by coverage and efficiency for aggressive selection
            levelCandidates.sort((a, b) -> {
                // Prioritize by covered required chunks (descending), then by efficiency
                int coverageCompare = Integer.compare(b.getRequiredChunkCount(), 
                                                     a.getRequiredChunkCount());
                if (coverageCompare != 0) return coverageCompare;
                return Double.compare(b.efficiency(), a.efficiency());
            });
            
            // Select the best candidates that meet our aggressive criteria
            for (NodeCandidate candidate : levelCandidates) {
                if (remainingChunks.isEmpty()) break;
                
                // Check if this candidate still covers remaining chunks
                if (candidate.coveredRequiredChunks().stream().anyMatch(remainingChunks::contains)) {
                    SchedulingReason reason = determineSchedulingReason(candidate, requiredChunks);
                    
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
                SchedulingReason.FALLBACK,
                priority++,
                byteRange.getLength(),
                List.of(chunkIndex),
                List.of(chunkIndex),
                "Fallback leaf node for chunk " + chunkIndex + " (no suitable internal node found)"
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
        
        // Find the range of chunks needed
        int startChunk = shape.getChunkIndexForPosition(offset);
        int endChunk = shape.getChunkIndexForPosition(Math.min(offset + length - 1, shape.getTotalContentSize() - 1));
        
        // Aggressively expand the range to include nearby chunks for prefetching
        int expandedStart = Math.max(0, startChunk - 2);
        int expandedEnd = Math.min(shape.getTotalChunks() - 1, endChunk + 4);
        
        // For aggressive scheduling, use selectOptimalNodes to get concurrent downloads
        List<Integer> requiredChunks = new ArrayList<>();
        for (int chunk = expandedStart; chunk <= expandedEnd; chunk++) {
            if (!state.isValid(chunk)) {
                requiredChunks.add(chunk);
            }
        }
        
        // If we have many chunks to download, schedule them concurrently
        if (!requiredChunks.isEmpty()) {
            List<SchedulingDecision> decisions = selectOptimalNodes(requiredChunks, shape, state);
            
            // Submit all tasks concurrently
            for (SchedulingDecision decision : decisions) {
                CompletableFuture<Void> future = schedulingTarget.getOrCreateFuture(decision.nodeIndex());
                
                MerkleShape.MerkleNodeRange byteRange = shape.getByteRangeForNode(decision.nodeIndex());
                MerkleShape.MerkleNodeRange leafRange = shape.getLeafRangeForNode(decision.nodeIndex());
                boolean isLeaf = shape.isLeafNode(decision.nodeIndex());
                
                NodeDownloadTask task = new AggressiveNodeDownloadTask(
                    decision.nodeIndex(),
                    byteRange.getStart(),
                    byteRange.getLength(),
                    isLeaf,
                    leafRange,
                    future
                );
                
                schedulingTarget.offerTask(task);
            }
        }
    }
    
    ///
    /// Finds node candidates at a specific tree level.
    ///
    /// @param shape The merkle shape
    /// @param state The merkle state
    /// @param expandedChunks All chunks being considered (including prefetch)
    /// @param remainingChunks Chunks that still need to be covered
    /// @param level Tree level to search (0 = root)
    /// @return List of node candidates at the specified level
    ///
    private List<NodeCandidate> findInternalNodeCandidatesAtLevel(
            MerkleShape shape, MerkleState state, Set<Integer> expandedChunks,
            Set<Integer> remainingChunks, int level) {
        
        List<NodeCandidate> candidates = new ArrayList<>();
        List<Integer> nodesAtLevel = shape.getInternalNodesAtLevel(level);
        
        for (int nodeIndex : nodesAtLevel) {
            // Skip nodes that might have invalid leaf ranges
            MerkleShape.MerkleNodeRange leafRange = shape.getLeafRangeForNode(nodeIndex);
            if (leafRange.getStart() >= shape.getTotalChunks()) {
                continue;
            }
            
            NodeCandidate candidate = evaluateNodeCandidate(
                shape, state, nodeIndex, expandedChunks, remainingChunks);
            
            if (candidate != null && candidate.getRequiredChunkCount() >= MIN_CHUNKS_FOR_INTERNAL_NODE 
                && candidate.efficiency() >= MIN_INTERNAL_NODE_EFFICIENCY) {
                candidates.add(candidate);
            }
        }
        
        return candidates;
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
        
        // Get all chunks covered by this node
        List<Integer> nodeChunks = shape.getChunksForNode(nodeIndex);
        
        for (Integer chunkIndex : nodeChunks) {
            if (!state.isValid(chunkIndex)) {
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
            "%s node %d covering %d required + %d prefetch chunks (efficiency %.2f)",
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
    /// Determines the scheduling reason for a node candidate.
    ///
    /// @param candidate The node candidate
    /// @param originalRequired The original required chunks (before expansion)
    /// @return The appropriate scheduling reason
    ///
    private SchedulingReason determineSchedulingReason(NodeCandidate candidate, List<Integer> originalRequired) {
        if (candidate.efficiency() >= 0.95) {
            return SchedulingReason.EXACT_MATCH;
        } else if (candidate.efficiency() >= 0.7) {
            return SchedulingReason.EFFICIENT_COVERAGE;
        } else if (candidate.getExcessChunkCount() > candidate.getRequiredChunkCount()) {
            return SchedulingReason.PREFETCH;
        } else if (candidate.getRequiredChunkCount() > 3) {
            return SchedulingReason.BANDWIDTH_OPTIMIZATION;
        } else {
            return SchedulingReason.CONSOLIDATION;
        }
    }
    
    
    
    /// Implementation of NodeDownloadTask for aggressive scheduling.
    private static class AggressiveNodeDownloadTask implements NodeDownloadTask {
        private final int nodeIndex;
        private final long offset;  
        private final long size;
        private final boolean isLeaf;
        private final MerkleShape.MerkleNodeRange leafRange;
        private final CompletableFuture<Void> future;
        
        public AggressiveNodeDownloadTask(int nodeIndex, long offset, long size, boolean isLeaf,
                                        MerkleShape.MerkleNodeRange leafRange, CompletableFuture<Void> future) {
            this.nodeIndex = nodeIndex;
            this.offset = offset;
            this.size = size;
            this.isLeaf = isLeaf;
            this.leafRange = leafRange;
            this.future = future;
        }
        
        @Override
        public int getNodeIndex() { return nodeIndex; }
        
        @Override
        public long getOffset() { return offset; }
        
        @Override
        public long getSize() { return size; }
        
        @Override
        public boolean isLeafNode() { return isLeaf; }
        
        @Override
        public MerkleShape.MerkleNodeRange getLeafRange() { return leafRange; }
        
        @Override
        public CompletableFuture<Void> getFuture() { return future; }
    }
}