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

import io.nosqlbench.vectordata.merklev2.MerkleRef;
import io.nosqlbench.vectordata.merklev2.MerkleShape;
import io.nosqlbench.vectordata.merklev2.MerkleState;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/// Simplified implementation of MerkleState for simulation testing.
/// 
/// This implementation provides in-memory tracking of chunk validation state
/// without the complexity of persistence or cryptographic verification.
/// It's designed for testing scheduler algorithms under various scenarios
/// of partial data availability.
/// 
/// Key features:
/// - Thread-safe chunk validation tracking
/// - Configurable initial state (empty, partially filled, etc.)
/// - Statistics tracking for simulation analysis
/// - No actual file I/O or persistence
/// 
/// Usage:
/// ```java
/// SimulatedMerkleShape shape = new SimulatedMerkleShape(fileSize, chunkSize);
/// SimulatedMerkleState state = new SimulatedMerkleState(shape);
/// 
/// // Simulate some chunks already downloaded
/// state.markChunkValid(0);
/// state.markChunkValid(5);
/// 
/// // Use with scheduler testing
/// boolean needsDownload = !state.isValid(3);
/// ```
public class SimulatedMerkleState implements MerkleState {
    
    private final MerkleShape merkleShape;
    private final BitSet validChunks;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    /// Statistics for simulation analysis
    private volatile long validationCount = 0;
    private volatile long saveCallbackInvocations = 0;
    private volatile boolean closed = false;
    
    /// Creates a simulated merkle state with all chunks initially invalid.
    /// 
    /// @param merkleShape The merkle shape defining the tree structure
    public SimulatedMerkleState(MerkleShape merkleShape) {
        this.merkleShape = merkleShape;
        this.validChunks = new BitSet(merkleShape.getTotalChunks());
    }
    
    /// Creates a simulated merkle state with specified initial validity.
    /// 
    /// @param merkleShape The merkle shape defining the tree structure
    /// @param initialValidChunks BitSet indicating which chunks are initially valid
    public SimulatedMerkleState(MerkleShape merkleShape, BitSet initialValidChunks) {
        this.merkleShape = merkleShape;
        this.validChunks = (BitSet) initialValidChunks.clone();
        
        // Count initially valid chunks
        this.validationCount = validChunks.cardinality();
    }
    
    @Override
    public boolean saveIfValid(int chunkIndex, ByteBuffer data, Consumer<ByteBuffer> saveCallback) {
        merkleShape.validateChunkIndex(chunkIndex);
        
        if (closed) {
            throw new IllegalStateException("MerkleState has been closed");
        }
        
        lock.writeLock().lock();
        try {
            // In simulation, we assume all provided data is valid
            // (no actual hash verification)
            saveCallback.accept(data);
            saveCallbackInvocations++;
            
            if (!validChunks.get(chunkIndex)) {
                validChunks.set(chunkIndex);
                validationCount++;
            }
            
            return true;
            
        } catch (Exception e) {
            // Save callback failed
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    public MerkleShape getMerkleShape() {
        return merkleShape;
    }
    
    @Override
    public BitSet getValidChunks() {
        lock.readLock().lock();
        try {
            return (BitSet) validChunks.clone();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public boolean isValid(int chunkIndex) {
        merkleShape.validateChunkIndex(chunkIndex);
        
        lock.readLock().lock();
        try {
            return validChunks.get(chunkIndex);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public void flush() {
        // No-op in simulation (no persistence)
    }
    
    @Override
    public void close() {
        closed = true;
        // No resources to clean up in simulation
    }
    
    @Override
    public MerkleRef toRef() {
        lock.readLock().lock();
        try {
            // Check if all chunks are valid
            int totalChunks = merkleShape.getTotalChunks();
            for (int i = 0; i < totalChunks; i++) {
                if (!validChunks.get(i)) {
                    throw new RuntimeException("Cannot create MerkleRef: chunk " + i + " is not valid");
                }
            }
            
            // Return a simulated reference
            return new SimulatedMerkleRef(merkleShape);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /// Marks a chunk as valid without going through saveIfValid.
    /// 
    /// This is useful for simulation setup where we want to pre-populate
    /// certain chunks as already downloaded.
    /// 
    /// @param chunkIndex The chunk to mark as valid
    public void markChunkValid(int chunkIndex) {
        merkleShape.validateChunkIndex(chunkIndex);
        
        lock.writeLock().lock();
        try {
            if (!validChunks.get(chunkIndex)) {
                validChunks.set(chunkIndex);
                validationCount++;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /// Marks a chunk as invalid.
    /// 
    /// This is useful for simulation scenarios where we want to simulate
    /// data corruption or cache invalidation.
    /// 
    /// @param chunkIndex The chunk to mark as invalid
    public void markChunkInvalid(int chunkIndex) {
        merkleShape.validateChunkIndex(chunkIndex);
        
        lock.writeLock().lock();
        try {
            if (validChunks.get(chunkIndex)) {
                validChunks.clear(chunkIndex);
                validationCount--;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /// Gets the number of chunks that are currently valid.
    /// 
    /// @return Count of valid chunks
    public int getValidChunkCount() {
        lock.readLock().lock();
        try {
            return validChunks.cardinality();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /// Gets the completion percentage (0.0 to 100.0).
    /// 
    /// @return Percentage of chunks that are valid
    public double getCompletionPercentage() {
        int totalChunks = merkleShape.getTotalChunks();
        if (totalChunks == 0) {
            return 100.0;
        }
        return (getValidChunkCount() * 100.0) / totalChunks;
    }
    
    /// Gets the total number of validation operations performed.
    /// 
    /// @return Validation count
    public long getValidationCount() {
        return validationCount;
    }
    
    /// Gets the total number of save callback invocations.
    /// 
    /// @return Save callback invocation count
    public long getSaveCallbackInvocations() {
        return saveCallbackInvocations;
    }
    
    /// Checks if this state has been closed.
    /// 
    /// @return true if closed
    public boolean isClosed() {
        return closed;
    }
    
    /// Creates a partial state for testing scenarios.
    /// 
    /// This factory method creates a state where only certain chunks are
    /// initially valid, useful for testing scheduler behavior with
    /// partially downloaded files.
    /// 
    /// @param merkleShape The merkle shape
    /// @param validRatio Ratio of chunks to mark as valid (0.0 to 1.0)
    /// @param seed Random seed for reproducible test scenarios
    /// @return A new SimulatedMerkleState with partial validity
    public static SimulatedMerkleState createPartialState(MerkleShape merkleShape, 
                                                         double validRatio, long seed) {
        if (validRatio < 0.0 || validRatio > 1.0) {
            throw new IllegalArgumentException("Valid ratio must be between 0.0 and 1.0");
        }
        
        int totalChunks = merkleShape.getTotalChunks();
        int validChunks = (int) (totalChunks * validRatio);
        
        BitSet initialValid = new BitSet(totalChunks);
        
        // Use deterministic random for reproducible tests
        java.util.Random random = new java.util.Random(seed);
        
        // Randomly select chunks to mark as valid
        for (int i = 0; i < validChunks; i++) {
            int chunkIndex;
            do {
                chunkIndex = random.nextInt(totalChunks);
            } while (initialValid.get(chunkIndex));
            
            initialValid.set(chunkIndex);
        }
        
        return new SimulatedMerkleState(merkleShape, initialValid);
    }
    
    /// Creates a state with specific chunks marked as valid.
    /// 
    /// @param merkleShape The merkle shape
    /// @param validChunkIndices Array of chunk indices to mark as valid
    /// @return A new SimulatedMerkleState with specified chunks valid
    public static SimulatedMerkleState createWithValidChunks(MerkleShape merkleShape, 
                                                            int... validChunkIndices) {
        BitSet initialValid = new BitSet(merkleShape.getTotalChunks());
        
        for (int chunkIndex : validChunkIndices) {
            merkleShape.validateChunkIndex(chunkIndex);
            initialValid.set(chunkIndex);
        }
        
        return new SimulatedMerkleState(merkleShape, initialValid);
    }
    
    /// Simple simulated MerkleRef implementation.
    private static class SimulatedMerkleRef implements MerkleRef {
        private final MerkleShape merkleShape;
        
        public SimulatedMerkleRef(MerkleShape merkleShape) {
            this.merkleShape = merkleShape;
        }
        
        @Override
        public MerkleShape getShape() {
            return merkleShape;
        }
        
        @Override
        public byte[] getHashForLeaf(int leafIndex) {
            // Return dummy hash for simulation
            return new byte[32]; // 32-byte hash
        }
        
        @Override
        public byte[] getHashForIndex(int index) {
            // Return dummy hash for simulation
            return new byte[32]; // 32-byte hash
        }
        
        @Override
        public java.util.List<byte[]> getPathToRoot(int leafIndex) {
            // Return dummy path for simulation
            java.util.List<byte[]> path = new java.util.ArrayList<>();
            path.add(new byte[32]); // leaf hash
            path.add(new byte[32]); // root hash
            return path;
        }
        
        @Override
        public MerkleState createEmptyState(java.nio.file.Path path) throws java.io.IOException {
            // Return a new empty simulated state
            return new SimulatedMerkleState(merkleShape);
        }
    }
}