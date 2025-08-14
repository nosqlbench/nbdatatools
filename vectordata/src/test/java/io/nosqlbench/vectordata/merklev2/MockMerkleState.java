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
import java.util.BitSet;
import java.util.function.Consumer;

///
/// Mock implementation of MerkleState for testing purposes.
/// 
/// This implementation provides a controllable MerkleState that can be
/// configured with specific validity patterns for testing scheduler
/// behavior under various conditions. It maintains all the essential
/// MerkleState functionality while allowing direct manipulation of
/// chunk validity for test scenarios.
///
public class MockMerkleState implements MerkleState {
    private final MerkleShape shape;
    private final BitSet validChunks;
    private boolean closed = false;
    
    ///
    /// Creates a mock MerkleState with the specified shape and validity pattern.
    ///
    /// @param shape The MerkleShape defining the content structure
    /// @param validChunks BitSet indicating which chunks are valid
    /// @throws IllegalArgumentException if parameters are null
    ///
    public MockMerkleState(MerkleShape shape, BitSet validChunks) {
        if (shape == null) {
            throw new IllegalArgumentException("Shape cannot be null");
        }
        if (validChunks == null) {
            throw new IllegalArgumentException("Valid chunks BitSet cannot be null");
        }
        
        this.shape = shape;
        this.validChunks = (BitSet) validChunks.clone();
    }
    
    ///
    /// Creates a mock MerkleState with all chunks initially invalid.
    ///
    /// @param shape The MerkleShape defining the content structure
    /// @throws IllegalArgumentException if shape is null
    ///
    public MockMerkleState(MerkleShape shape) {
        this(shape, new BitSet());
    }
    
    @Override
    public MerkleShape getMerkleShape() {
        checkNotClosed();
        return shape;
    }
    
    @Override
    public boolean isValid(int chunkIndex) {
        checkNotClosed();
        shape.validateChunkIndex(chunkIndex);
        return validChunks.get(chunkIndex);
    }
    
    @Override
    public BitSet getValidChunks() {
        checkNotClosed();
        return (BitSet) validChunks.clone();
    }
    
    @Override
    public boolean saveIfValid(int chunkIndex, ByteBuffer data, Consumer<ByteBuffer> onSave) {
        checkNotClosed();
        shape.validateChunkIndex(chunkIndex);
        
        // For testing purposes, we'll simulate successful validation
        // In a real implementation, this would verify the hash
        if (!validChunks.get(chunkIndex)) {
            // Mark as valid and call the save callback
            validChunks.set(chunkIndex);
            if (onSave != null) {
                onSave.accept(data);
            }
            return true;
        }
        
        // Already valid - no need to save
        return false;
    }
    
    @Override
    public void flush() {
        checkNotClosed();
        // Mock implementation - nothing to flush
    }
    
    @Override
    public void close() {
        closed = true;
    }
    
    @Override
    public MerkleRef toRef() {
        checkNotClosed();
        // For testing purposes, we'll throw an exception if not all chunks are valid
        if (getInvalidChunkCount() > 0) {
            throw new IllegalStateException(
                "Cannot create MerkleRef: " + getInvalidChunkCount() + " chunks are still invalid");
        }
        
        // Return a mock MerkleRef - in real implementation this would be more complex
        throw new UnsupportedOperationException(
            "MockMerkleState.toRef() is not implemented - use a real MerkleState for this operation");
    }
    
    ///
    /// Manually sets the validity of a chunk (for testing).
    ///
    /// @param chunkIndex The chunk index to modify
    /// @param valid Whether the chunk should be valid or invalid
    /// @throws IllegalArgumentException if chunkIndex is out of bounds
    /// @throws IllegalStateException if this state is closed
    ///
    public void setChunkValid(int chunkIndex, boolean valid) {
        checkNotClosed();
        shape.validateChunkIndex(chunkIndex);
        
        if (valid) {
            validChunks.set(chunkIndex);
        } else {
            validChunks.clear(chunkIndex);
        }
    }
    
    ///
    /// Sets multiple chunks as valid or invalid (for testing).
    ///
    /// @param valid Whether the chunks should be valid or invalid
    /// @param chunkIndices The chunk indices to modify
    /// @throws IllegalArgumentException if any chunkIndex is out of bounds
    /// @throws IllegalStateException if this state is closed
    ///
    public void setChunksValid(boolean valid, int... chunkIndices) {
        checkNotClosed();
        for (int chunkIndex : chunkIndices) {
            setChunkValid(chunkIndex, valid);
        }
    }
    
    ///
    /// Sets a range of chunks as valid or invalid (for testing).
    ///
    /// @param startChunk Starting chunk index (inclusive)
    /// @param endChunk Ending chunk index (exclusive)
    /// @param valid Whether the chunks should be valid or invalid
    /// @throws IllegalArgumentException if range is invalid
    /// @throws IllegalStateException if this state is closed
    ///
    public void setChunkRangeValid(int startChunk, int endChunk, boolean valid) {
        checkNotClosed();
        if (startChunk < 0 || endChunk < startChunk || endChunk > shape.getTotalChunks()) {
            throw new IllegalArgumentException(
                "Invalid chunk range: [" + startChunk + ", " + endChunk + ") for " + 
                shape.getTotalChunks() + " chunks");
        }
        
        if (valid) {
            validChunks.set(startChunk, endChunk);
        } else {
            validChunks.clear(startChunk, endChunk);
        }
    }
    
    ///
    /// Marks all chunks as valid (for testing).
    ///
    /// @throws IllegalStateException if this state is closed
    ///
    public void setAllChunksValid() {
        checkNotClosed();
        validChunks.set(0, shape.getTotalChunks());
    }
    
    ///
    /// Marks all chunks as invalid (for testing).
    ///
    /// @throws IllegalStateException if this state is closed
    ///
    public void setAllChunksInvalid() {
        checkNotClosed();
        validChunks.clear();
    }
    
    ///
    /// Gets the number of valid chunks.
    ///
    /// @return Count of chunks marked as valid
    /// @throws IllegalStateException if this state is closed
    ///
    public int getValidChunkCount() {
        checkNotClosed();
        return validChunks.cardinality();
    }
    
    ///
    /// Gets the number of invalid chunks.
    ///
    /// @return Count of chunks marked as invalid
    /// @throws IllegalStateException if this state is closed
    ///
    public int getInvalidChunkCount() {
        checkNotClosed();
        return shape.getTotalChunks() - validChunks.cardinality();
    }
    
    ///
    /// Checks if this mock state has been closed.
    ///
    /// @return true if closed, false otherwise
    ///
    public boolean isClosed() {
        return closed;
    }
    
    ///
    /// Creates a snapshot of the current validity state.
    ///
    /// @return A copy of the current valid chunks BitSet
    /// @throws IllegalStateException if this state is closed
    ///
    public BitSet createValiditySnapshot() {
        checkNotClosed();
        return (BitSet) validChunks.clone();
    }
    
    ///
    /// Restores validity state from a snapshot.
    ///
    /// @param snapshot The validity snapshot to restore
    /// @throws IllegalArgumentException if snapshot is null
    /// @throws IllegalStateException if this state is closed
    ///
    public void restoreValiditySnapshot(BitSet snapshot) {
        checkNotClosed();
        if (snapshot == null) {
            throw new IllegalArgumentException("Snapshot cannot be null");
        }
        
        validChunks.clear();
        validChunks.or(snapshot);
    }
    
    ///
    /// Ensures this state is not closed.
    ///
    /// @throws IllegalStateException if this state is closed
    ///
    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("MockMerkleState is closed");
        }
    }
    
    @Override
    public String toString() {
        if (closed) {
            return "MockMerkleState{closed}";
        }
        return String.format("MockMerkleState{chunks=%d, valid=%d, invalid=%d}",
            shape.getTotalChunks(), getValidChunkCount(), getInvalidChunkCount());
    }
}