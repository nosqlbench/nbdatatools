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


import java.io.IOException;
import java.nio.file.Path;

/// Provides a read-only view of invariant merkle tree reference data.
/// 
/// MerkleRef represents the authoritative reference tree that contains known-good
/// hashes for data validation. This interface is used to verify downloaded chunks
/// against their expected hashes and to retrieve reference tree metadata.
/// 
/// The reference tree is immutable and serves as the "golden" copy for integrity
/// verification. All data downloads and validations should be performed against
/// the reference tree to ensure data consistency and detect corruption.
public interface MerkleRef {


  /// Gets the shape descriptor that defines the structure and geometry of this merkle tree.
  /// 
  /// The MerkleShape provides authoritative calculations for chunk boundaries,
  /// tree dimensions, and position mappings. This ensures consistent chunk
  /// handling across all components that interact with this reference tree.
  /// 
  /// @return The MerkleShape instance describing the tree structure and chunk layout
  public MerkleShape getShape();

  /// Retrieves the reference hash for a specific leaf node (data chunk).
  /// 
  /// Leaf nodes in the merkle tree correspond to data chunks. This method returns
  /// the known-good hash that should match the hash of the actual chunk data.
  /// These hashes are used for verification during chunk download and validation.
  /// 
  /// @param leafIndex The index of the leaf node (corresponds to chunk index, 0-based)
  /// @return The reference hash bytes for the specified leaf, or null if not available
  /// @throws IllegalArgumentException if leafIndex is out of bounds
  public byte[] getHashForLeaf(int leafIndex);

  /// Retrieves the hash for any node in the merkle tree by its tree index.
  /// 
  /// This method provides access to both internal node hashes and leaf node hashes
  /// using the tree's internal indexing scheme. Internal nodes contain computed
  /// hashes from their children and are used for efficient merkle proof generation
  /// and tree validation.
  /// 
  /// The tree uses a flat array representation where:
  /// - Internal nodes occupy indices [0, offset)
  /// - Leaf nodes occupy indices [offset, nodeCount)
  /// 
  /// @param index The tree index of the node (follows internal tree indexing)
  /// @return The hash bytes for the specified node, or null if not available
  /// @throws IllegalArgumentException if index is out of bounds
  public byte[] getHashForIndex(int index);

  /// Gets the path from a leaf node to the root node.
  /// 
  /// This method returns the complete merkle path from the specified leaf
  /// to the root node, which can be used for merkle proof verification.
  /// The first element in the list is the leaf hash, and the last element
  /// is the root hash.
  /// 
  /// @param leafIndex The index of the leaf node (0-based)
  /// @return A list of hash bytes representing the path from leaf to root
  /// @throws IllegalArgumentException if leafIndex is out of bounds
  java.util.List<byte[]> getPathToRoot(int leafIndex);

  /// This creates a MerkleState from the given MerkleRef which includes all the details of the
  /// MerkleRef except the valid bits are cleared. The file is initially persisted into the
  /// specified file before this method returns.
  MerkleState createEmptyState(Path path) throws IOException;
}
