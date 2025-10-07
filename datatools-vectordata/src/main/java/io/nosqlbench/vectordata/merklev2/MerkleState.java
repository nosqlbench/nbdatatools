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
import java.util.BitSet;
import java.util.function.Consumer;

/// This is a merkle tree state interface. It is used to keep track of which chunks of
/// related content are valid, depending on a reference merkle tree. Merkle state should be
/// persisted in files ending in '.mrkl' and the reference merkle trees should be in files ending
///  in '.mref'. The reference merkle trees are never changed. A MerkleState file is always
/// instantiated from a reference merkle tree and maintains the same MerkleShape. When a
/// MerkleState is created from a MerkleTree, The hash values are all copied into the MerkleState
/// first, and then the valid bits are all set to false. This is because the reference tree is always
/// considered valid, and the state tree bitset is actively updated as chunks are downloaded and
/// verified against the hashes which originally came from the reference tree.
///
/// A MerkleState object can be constructed from two different scenarios:
/// 1. There is no previous state file, so one is initialized with the same geometry as the
/// reference tree. In this case, all hashes are invalid in the merkle state and all bits are set
///  to false.
/// 2. There is a previous state file, so it is loaded and used as the starting point. In this case,
/// the valid bits are set according to the state file.
public interface MerkleState extends AutoCloseable {

  /// Submit a buffer of data for a given index. If the hash of the data is equal to the same
  /// indexed hash of the underlying MerkleRef  then the following should
  /// happen:
  /// 1. The saveCallback should be called with the data. If this is unsuccessful, an exception
  /// should be thrown. If it is successful, then proceed...
  /// 2. The hash of the reference should be copied to the MerkleState hash tree.
  /// 3. The valid bit for that index should be set to true.
  /// 4. The merkle state tree should be forced to with the flush() method.
  /// @return true if the data was saved and the merkle state was updated, false otherwise.
  /// @param chunkIndex The index of the chunk to save
  /// @param data The data to save
  /// @param saveCallback The callback to save the data
  public boolean saveIfValid(int chunkIndex, ByteBuffer data, Consumer<ByteBuffer> saveCallback);

  /// Get the merkle shape of this state
  /// @return The merkle shape
  public MerkleShape getMerkleShape();

  /// Get the BitSet of valid chunks
  /// @return The BitSet of valid chunks
  public BitSet getValidChunks();

  /// Check if a specific chunk is valid
  /// @param chunkIndex The index of the chunk to check
  /// @return true if the chunk is valid, false otherwise
  public boolean isValid(int chunkIndex);

  /// Flush any pending changes to disk
  public void flush();

  /// Close the merkle state
  public void close();

  /// Creates a MerkleRef from this MerkleState if all chunks have been validated.
  /// This method checks that all chunks are marked as valid (validated and saved)
  /// before creating the reference. If any chunks are still invalid, throws an exception.
  ///
  /// @return A MerkleRef interface view of this fully validated state
  /// @throws IncompleteMerkleStateException If not all chunks have been validated
  public MerkleRef toRef();

  // Static factory methods for creating MerkleState instances

  /// Creates a MerkleState from an existing MerkleRef.
  /// This initializes a new state file with all chunks marked as invalid (not yet verified).
  /// The state file is created and persisted before this method returns.
  /// 
  /// This method delegates to MerkleDataImpl.createStateFromRef() for the actual implementation.
  /// For direct access to the factory method, use MerkleDataImpl.createStateFromRef() instead.
  /// 
  /// @param merkleRef The reference merkle tree to base the state on
  /// @param statePath The path where the .mrkl state file will be created
  /// @return A new MerkleState based on the reference tree
  /// @throws IOException If an I/O error occurs during creation
  static MerkleState fromRef(MerkleRef merkleRef, Path statePath) throws IOException {
    return MerkleDataImpl.createStateFromRef(merkleRef, statePath);
  }

  /// Loads an existing MerkleState from a .mrkl file.
  /// This restores a previously saved state including validation progress.
  /// 
  /// @param statePath The path to the .mrkl state file
  /// @return The loaded MerkleState with preserved validation state
  /// @throws IOException If an I/O error occurs during loading
  static MerkleState load(Path statePath) throws IOException {
    // MerkleRefFactory.load returns MerkleDataImpl which implements both interfaces
    // When loading a .mrkl file, we return it as MerkleState interface
    return MerkleRefFactory.load(statePath);
  }
}
