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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/// Conservative chunk scheduler that minimizes unnecessary downloads.
/// 
/// This scheduler prioritizes precision over bandwidth utilization,
/// downloading only the exact chunks needed and avoiding any wasteful
/// over-downloading. It's optimized for low-bandwidth or high-latency
/// connections where every byte counts.
/// 
/// Strategy:
/// - Download only the minimum required chunks
/// - Prefer individual leaf nodes over internal nodes
/// - No speculative prefetching
/// - Minimize bandwidth usage at the cost of more requests
/// 
/// Best suited for:
/// - Low-bandwidth connections (< 10 Mbps)
/// - High-latency networks (> 200ms)
/// - Sparse or unpredictable access patterns
/// - Scenarios where bandwidth is limited or expensive
public class ConservativeChunkScheduler implements ChunkScheduler {
    
    @Override
    public List<SchedulingDecision> selectOptimalNodes(
            List<Integer> requiredChunks,
            MerkleShape shape,
            MerkleState state) {
        
        if (requiredChunks.isEmpty()) {
            return List.of();
        }
        
        List<SchedulingDecision> decisions = new ArrayList<>();
        int priority = 0;
        
        // Conservative approach: use individual leaf nodes only, no consolidation or prefetching
        for (Integer chunkIndex : requiredChunks) {
            // Skip chunks that are already valid
            if (state.isValid(chunkIndex)) {
                continue;
            }
            
            int leafNodeIndex = shape.chunkIndexToLeafNode(chunkIndex);
            MerkleShape.MerkleNodeRange byteRange = shape.getByteRangeForNode(leafNodeIndex);
            
            // Conservative scheduler always uses MINIMAL_DOWNLOAD for individual chunks
            SchedulingDecision decision = new SchedulingDecision(
                leafNodeIndex,
                SchedulingReason.MINIMAL_DOWNLOAD,
                priority++,
                byteRange.getLength(),
                List.of(chunkIndex),
                List.of(chunkIndex),
                "Conservative leaf node download for chunk " + chunkIndex + " (no prefetch, minimal bandwidth)"
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
        
        // Find the exact range of chunks needed (no expansion)
        int startChunk = shape.getChunkIndexForPosition(offset);
        int endChunk = shape.getChunkIndexForPosition(Math.min(offset + length - 1, shape.getTotalContentSize() - 1));
        
        // Download only the chunks that are actually needed and invalid
        for (int chunkIndex = startChunk; chunkIndex <= endChunk; chunkIndex++) {
            if (state.isValid(chunkIndex)) {
                continue; // Skip already valid chunks
            }
            
            int leafNodeIndex = shape.chunkIndexToLeafNode(chunkIndex);
            
            // Get or create a future for this node from the SchedulingTarget
            CompletableFuture<Void> future = schedulingTarget.getOrCreateFuture(leafNodeIndex);
            
            // Create conservative download task for individual chunk
            MerkleShape.MerkleNodeRange byteRange = shape.getByteRangeForNode(leafNodeIndex);
            MerkleShape.MerkleNodeRange leafRange = shape.getLeafRangeForNode(leafNodeIndex);
            
            NodeDownloadTask task = new ConservativeNodeDownloadTask(
                leafNodeIndex,
                byteRange.getStart(),
                byteRange.getLength(),
                true, // Always leaf nodes
                leafRange,
                future
            );
            
            schedulingTarget.offerTask(task);
        }
    }
    
    /// Implementation of NodeDownloadTask for conservative scheduling.
    private static class ConservativeNodeDownloadTask implements NodeDownloadTask {
        private final int nodeIndex;
        private final long offset;
        private final long size;
        private final boolean isLeaf;
        private final MerkleShape.MerkleNodeRange leafRange;
        private final CompletableFuture<Void> future;
        
        public ConservativeNodeDownloadTask(int nodeIndex, long offset, long size, boolean isLeaf,
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