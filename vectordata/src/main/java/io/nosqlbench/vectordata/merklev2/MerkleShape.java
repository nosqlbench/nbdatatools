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

// ChunkBoundary is now in the same package

/**
 * Defines the shape and geometry of a merkle tree structure.
 * 
 * This interface provides the authoritative source for all chunk-related calculations
 * and merkle tree dimensions. It ensures consistency across all merkle system components
 * by centralizing chunk boundary calculations, position mappings, and tree structure
 * parameters.
 * 
 * The MerkleShape abstracts the mathematical relationships between:
 * - Content size and optimal chunk size
 * - Chunk indices and file positions
 * - Tree structure (leaf nodes, internal nodes, capacity)
 * - Validation boundaries for safe operations
 */
public interface MerkleShape {

  /**
   * Gets the starting position (inclusive) of the specified chunk.
   * 
   * This is the authoritative calculation for chunk start positions.
   * All merkle components must use this method to ensure consistent chunk boundaries.
   * 
   * @param chunkIndex The index of the chunk (0-based)
   * @return The starting byte position of the chunk (inclusive)
   * @throws IllegalArgumentException if chunkIndex is out of bounds [0, getTotalChunks())
   */
  long getChunkStartPosition(int chunkIndex);

  /**
   * Gets the ending position (exclusive) of the specified chunk.
   * 
   * This is the authoritative calculation for chunk end positions.
   * All merkle components must use this method to ensure consistent chunk boundaries.
   * 
   * @param chunkIndex The index of the chunk (0-based)
   * @return The ending byte position of the chunk (exclusive)
   * @throws IllegalArgumentException if chunkIndex is out of bounds [0, getTotalChunks())
   */
  long getChunkEndPosition(int chunkIndex);

  /**
   * Gets the actual size of the specified chunk in bytes.
   * 
   * This is the authoritative calculation for individual chunk sizes.
   * Most chunks will be the same size (getChunkSize()), but the last chunk
   * may be smaller if the content size is not evenly divisible by chunk size.
   * 
   * @param chunkIndex The index of the chunk (0-based)
   * @return The actual size of the chunk in bytes
   * @throws IllegalArgumentException if chunkIndex is out of bounds [0, getTotalChunks())
   */
  long getActualChunkSize(int chunkIndex);

  /**
   * Gets the chunk index that contains the specified content position.
   * 
   * This is the authoritative calculation for position-to-chunk mapping that
   * determines which chunk contains a given file position. Used for determining
   * which chunk needs to be downloaded for a specific read operation.
   * 
   * @param contentPosition The position in the content (0-based byte offset)
   * @return The chunk index containing this position
   * @throws IllegalArgumentException if contentPosition is out of bounds [0, getTotalContentSize())
   */
  int getChunkIndexForPosition(long contentPosition);

  /**
   * Validates that a chunk index is within the valid range.
   * 
   * Ensures the chunk index is within bounds [0, getTotalChunks()) and throws
   * an exception if not. This provides early validation to prevent out-of-bounds
   * access in merkle operations.
   * 
   * @param chunkIndex The chunk index to validate
   * @throws IllegalArgumentException if the index is out of bounds
   */
  void validateChunkIndex(int chunkIndex);

  /**
   * Validates that a content position is within the valid range.
   * 
   * Ensures the content position is within bounds [0, getTotalContentSize()) and
   * throws an exception if not. This provides early validation for file position
   * access.
   * 
   * @param contentPosition The content position to validate
   * @throws IllegalArgumentException if the position is out of bounds
   */
  void validateContentPosition(long contentPosition);

  /**
   * Gets the chunk size in bytes.
   * 
   * Returns the size of each chunk in bytes. The chunk size is typically
   * a power of 2 between 1MB and 64MB, automatically calculated to maintain
   * optimal chunk counts for the content size.
   * 
   * @return The size of each chunk in bytes
   */
  long getChunkSize();

  /**
   * Gets the total content size in bytes.
   * 
   * Returns the total size of the content that this merkle tree represents.
   * This is the virtual file size that clients will see, regardless of how
   * much data has actually been downloaded and cached locally.
   * 
   * @return The total size of the content in bytes
   */
  long getTotalContentSize();

  /**
   * Gets the total number of chunks.
   * 
   * Returns the number of chunks required to represent the entire content.
   * This is calculated as ceil(totalContentSize / chunkSize) and represents
   * the number of data chunks that can be downloaded and verified.
   * 
   * @return The total number of chunks required to represent the content
   */
  int getTotalChunks();

  /**
   * Gets the number of leaf nodes in the merkle tree.
   * 
   * In the merkle tree structure, leaf nodes correspond to data chunks.
   * This value is typically equal to getTotalChunks() as each chunk
   * corresponds to one leaf node in the tree.
   * 
   * @return The number of leaf nodes in the merkle tree
   */
  int getLeafCount();

  /**
   * Gets the capacity for leaf nodes (next power of 2 >= leafCount).
   * 
   * Returns the capacity allocated for leaf nodes in the merkle tree structure.
   * This is the next power of 2 greater than or equal to the leaf count,
   * which determines the tree's balanced structure and affects node indexing.
   * 
   * @return The leaf node capacity (power of 2)
   */
  int getCapLeaf();

  /**
   * Gets the total number of nodes in the merkle tree.
   * 
   * Returns the total number of nodes (both internal and leaf nodes) in the
   * complete merkle tree structure. This determines the size of arrays needed
   * to store the tree and affects memory allocation for tree operations.
   * 
   * @return The total node count (internal nodes + leaf nodes)
   */
  int getNodeCount();

  /**
   * Gets the offset where leaf nodes start in the merkle tree array.
   * 
   * In a flat array representation of the merkle tree, this returns the
   * index where leaf nodes begin. Internal nodes occupy indices [0, offset)
   * and leaf nodes occupy indices [offset, nodeCount). This is crucial for
   * correct tree traversal and node addressing.
   * 
   * @return The offset of the first leaf node in the tree array
   */
  int getOffset();

  /**
   * Gets the number of internal (non-leaf) nodes in the merkle tree.
   * 
   * Returns the count of internal nodes that contain computed hashes from
   * their children. These nodes form the verification path structure used
   * for efficient merkle proofs and tree validation.
   * 
   * @return The internal node count
   */
  int getInternalNodeCount();

  /**
   Factory method to create MerkleShape instances from content size.

   This is the canonical way to create MerkleShape instances. It automatically
   calculates optimal chunk sizes and tree parameters based on the content size,
   ensuring consistent behavior across all merkle system components.

   The implementation will:
   - Choose an optimal chunk size (power of 2, between 1MB-64MB)
   - Keep chunk counts reasonable (typically ≤ 4096 chunks)
   - Calculate all tree structure parameters consistently
   @param contentSize
   The total size of the content in bytes (must be non-negative)
   @return A MerkleShape instance configured for the specified content size
   @throws IllegalArgumentException
   if contentSize is negative
   */
  static BaseMerkleShape fromContentSize(long contentSize) {
    return new BaseMerkleShape(contentSize);
  }

  ChunkBoundary getChunkBoundary(int chunkIndex);
}
