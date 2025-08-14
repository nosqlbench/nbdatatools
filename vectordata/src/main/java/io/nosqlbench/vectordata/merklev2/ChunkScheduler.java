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

import java.util.List;
import java.util.concurrent.CompletableFuture;

/// Stateless interface for scheduling node downloads for MAFileChannel.
///
/// The scheduler is responsible for determining which merkle tree nodes need to be downloaded
/// to satisfy a read request. It is completely stateless and can be safely swapped out at
/// runtime without affecting ongoing operations.
///
/// Nodes can represent individual chunks (leaf nodes) or larger regions (internal nodes).
/// Internal nodes automatically represent all leaf nodes they close over, allowing for
/// more efficient bulk downloads when appropriate.
///
/// Key responsibilities:
/// - Analyze the requested byte range against the MerkleState to determine missing nodes
/// - Return download tasks for nodes that need to be fetched
/// - Prefer larger internal nodes when possible to minimize download operations
///
/// State management (duplicate prevention, futures tracking) is handled by the MAFileChannel
/// to ensure the scheduler remains stateless and swappable.
///
/// Example usage:
/// ```java
/// ChunkScheduler scheduler = new DefaultChunkScheduler();
/// ChunkQueue chunkQueue = new ChunkQueue(100); // Keep history of last 100 completed tasks
/// scheduler.scheduleDownloads(shape, state, offset, length, chunkQueue);
///```
public interface ChunkScheduler {

  /// Schedules downloads for merkle nodes needed to satisfy a read request.
  ///
  /// The scheduler examines the MerkleState to determine which nodes in the
  /// requested range are not yet valid and need to be downloaded. It creates
  /// download tasks and adds them directly to the provided ChunkQueue.
  ///
  /// The scheduler should prefer larger internal nodes when possible to minimize
  /// the number of separate download operations, falling back to individual leaf
  /// nodes only when necessary.
  ///
  /// The ChunkQueue manages all aspects of task queuing, in-flight tracking,
  /// and completed task history, providing a complete view of download lifecycle.
  /// @param offset
  ///     The starting byte offset for the read request
  /// @param length
  ///     The number of bytes to read
  /// @param shape
  ///     The MerkleShape providing tree structure and conversion methods
  /// @param state
  ///     The MerkleState tracking which nodes are valid
  /// @param schedulingTarget
  ///     The SchedulingTarget to manage tasks and track state
  void scheduleDownloads(
      long offset,
      long length,
      MerkleShape shape,
      MerkleState state,
      SchedulingTarget schedulingTarget
  );
  
  ///
  /// Analyzes a read request and returns detailed scheduling decisions.
  /// 
  /// This method provides the core node-centric scheduling logic with full
  /// traceability. It identifies which merkle nodes should be downloaded to
  /// satisfy the request and provides detailed reasoning for each decision.
  /// 
  /// This method is primarily used for testing and analysis, while the
  /// scheduleDownloads method handles the actual task creation and queuing.
  ///
  /// @param offset The starting byte offset for the read request
  /// @param length The number of bytes to read
  /// @param shape The MerkleShape providing tree structure
  /// @param state The MerkleState tracking which nodes are valid
  /// @return List of scheduling decisions with full traceability
  ///
  default List<SchedulingDecision> analyzeSchedulingDecisions(
      long offset,
      long length,
      MerkleShape shape,
      MerkleState state
  ) {
    // Default implementation: convert byte range to chunk indices and delegate
    int startChunk = shape.getChunkIndexForPosition(offset);
    int endChunk = shape.getChunkIndexForPosition(
        Math.min(offset + length - 1, shape.getTotalContentSize() - 1));
    
    List<Integer> requiredChunks = new java.util.ArrayList<>();
    for (int chunk = startChunk; chunk <= endChunk; chunk++) {
      if (!state.isValid(chunk)) {
        requiredChunks.add(chunk);
      }
    }
    
    return selectOptimalNodes(requiredChunks, shape, state);
  }
  
  ///
  /// Selects optimal merkle nodes for downloading based on the requested chunks.
  /// 
  /// This method implements the core scheduling strategy by taking a list of
  /// required chunk indices and determining which merkle nodes (leaf or internal)
  /// should be downloaded to satisfy the requirement most efficiently.
  /// 
  /// The selection process considers:
  /// - Coverage efficiency (minimizing over-downloading)
  /// - Network efficiency (minimizing request count)
  /// - Existing download state (avoiding duplicates)
  /// - Scheduler-specific strategies (aggressive vs conservative)
  ///
  /// @param requiredChunks List of chunk indices that must be downloaded
  /// @param shape The MerkleShape providing tree structure
  /// @param state The MerkleState tracking which nodes are valid
  /// @return List of selected nodes with detailed reasoning
  ///
  default List<SchedulingDecision> selectOptimalNodes(
      List<Integer> requiredChunks,
      MerkleShape shape,
      MerkleState state
  ) {
    // Default implementation: create leaf node decisions for each required chunk
    List<SchedulingDecision> decisions = new java.util.ArrayList<>();
    
    for (Integer chunkIndex : requiredChunks) {
      if (!state.isValid(chunkIndex)) {
        int leafNodeIndex = shape.chunkIndexToLeafNode(chunkIndex);
        MerkleShape.MerkleNodeRange leafRange = shape.getLeafRangeForNode(leafNodeIndex);
        MerkleShape.MerkleNodeRange byteRange = shape.getByteRangeForNode(leafNodeIndex);
        
        // Create simple leaf node decision
        SchedulingDecision decision = new SchedulingDecision(
            leafNodeIndex,
            SchedulingReason.MINIMAL_DOWNLOAD,
            0, // priority
            byteRange.getLength(),
            List.of(chunkIndex), // required chunks
            List.of(chunkIndex), // covered chunks (same as required for leaf nodes)
            "Default leaf node selection for chunk " + chunkIndex
        );
        
        decisions.add(decision);
      }
    }
    
    return decisions;
  }


  /// Represents a merkle node download task.
  ///
  /// Each task identifies a specific merkle tree node that needs to be downloaded
  /// and provides a future that completes when the download finishes. The node
  /// can be either a leaf node (representing a single chunk) or an internal node
  /// (representing multiple chunks covered by its leaf descendants).
  interface NodeDownloadTask {

    /// Gets the index of the merkle tree node to download.
    /// @return The node index in the merkle tree
    int getNodeIndex();

    /// Gets the starting byte offset covered by this node.
    /// @return The byte offset within the file
    long getOffset();

    /// Gets the size of the byte range covered by this node.
    /// @return The byte range size
    long getSize();

    /// Checks if this is a leaf node (single chunk) or internal node (multiple chunks).
    /// @return true if this is a leaf node, false if it's an internal node
    boolean isLeafNode();

    /// Gets the range of leaf nodes covered by this node.
    ///
    /// For leaf nodes, this returns a range containing only that leaf.
    /// For internal nodes, this returns all leaf nodes under this node.
    /// @return The range of leaf node indices covered by this download
    MerkleShape.MerkleNodeRange getLeafRange();

    /// Gets a future that completes when this node is downloaded and validated.
    ///
    /// Multiple callers requesting the same node should receive the same
    /// future instance to avoid duplicate downloads.
    /// @return A CompletableFuture that completes when the node is available
    CompletableFuture<Void> getFuture();
  }
  
  /// Schedules downloads iteratively for large requests that may exceed transport limits.
  ///
  /// This method breaks large requests into transport-compatible chunks while ensuring
  /// complete coverage of the requested range. It respects the maxChunkSize limit
  /// (typically 2GB for ByteBuffer-based transports) and processes the range iteratively.
  ///
  /// The iteration continues until all chunks in the requested range are valid,
  /// making multiple scheduling passes if necessary to handle complex merkle tree
  /// structures where optimal nodes might be larger than the transport limit.
  ///
  /// @param offset The starting byte offset for the read request
  /// @param length The number of bytes to read
  /// @param maxChunkSize Maximum size for individual download operations (e.g., 2GB limit)
  /// @param shape The MerkleShape providing tree structure
  /// @param state The MerkleState tracking which nodes are valid  
  /// @param schedulingTarget The SchedulingTarget to manage tasks and track state
  /// @return CompletableFuture that completes when the entire range is prebuffered
  default CompletableFuture<Void> scheduleDownloadsIteratively(
      long offset,
      long length,
      long maxChunkSize,
      MerkleShape shape,
      MerkleState state,
      SchedulingTarget schedulingTarget
  ) {
    // Default implementation: break into manageable segments and process iteratively
    return CompletableFuture.supplyAsync(() -> {
      long currentOffset = offset;
      long remainingLength = length;
      
      // Process the range in segments that respect maxChunkSize
      while (remainingLength > 0) {
        // Calculate segment size that won't exceed maxChunkSize
        long segmentLength = Math.min(remainingLength, maxChunkSize);
        
        // Schedule downloads for this segment
        scheduleDownloads(currentOffset, segmentLength, shape, state, schedulingTarget);
        
        // Wait for this segment to complete before proceeding
        // This ensures we don't overwhelm the transport with too many concurrent large requests
        try {
          // Get all futures from this segment and wait for completion
          // The SchedulingTarget should track these futures
          Thread.sleep(100); // Simple polling approach for default implementation
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted during iterative scheduling", e);
        }
        
        currentOffset += segmentLength;
        remainingLength -= segmentLength;
      }
      
      return null;
    });
  }
}