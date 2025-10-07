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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

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

  // Static factory methods for creating MerkleRef instances

  /// Creates a MerkleRef from a data file with progress tracking.
  /// This is the recommended method for creating merkle trees with progress monitoring.
  /// 
  /// @param dataPath The path to the data file
  /// @return A MerkleRefBuildProgress that tracks the building process
  /// @throws IOException If an I/O error occurs
  static MerkleRefBuildProgress fromData(Path dataPath) throws IOException {
    return MerkleRefFactory.fromData(dataPath);
  }

  /// Creates a MerkleRef from a data file without progress tracking.
  /// This method is simpler but doesn't provide progress information.
  /// 
  /// @param dataPath The path to the data file
  /// @return A CompletableFuture that will complete with the MerkleRef
  /// @throws IOException If an I/O error occurs
  static CompletableFuture<MerkleRef> fromDataSimple(Path dataPath) throws IOException {
    return MerkleRefFactory.fromDataSimple(dataPath).thenApply(impl -> (MerkleRef) impl);
  }

  /// Creates a MerkleRef from a ByteBuffer without progress tracking.
  /// 
  /// @param data The data buffer
  /// @return A CompletableFuture that will complete with the MerkleRef
  static CompletableFuture<MerkleRef> fromData(ByteBuffer data) {
    return MerkleRefFactory.fromData(data).thenApply(impl -> (MerkleRef) impl);
  }

  /// Loads a MerkleRef from an existing .mref file.
  /// 
  /// @param path The path to the .mref file
  /// @return A loaded MerkleRef
  /// @throws IOException If an I/O error occurs during loading
  static MerkleRef load(Path path) throws IOException {
    return MerkleRefFactory.load(path);
  }

  /// Creates an empty MerkleRef for the given content size.
  /// All hashes will be null/empty.
  /// 
  /// @param contentSize The total size of the content
  /// @return An empty MerkleRef
  static MerkleRef createEmpty(long contentSize) {
    return MerkleRefFactory.createEmpty(contentSize);
  }
}
