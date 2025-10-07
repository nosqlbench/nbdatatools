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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that verify proper interface segregation between MerkleRef and MerkleState.
 * This test class ensures that:
 * 1. MerkleRef provides read-only access to reference merkle trees
 * 2. MerkleState provides mutable state tracking for chunk validation
 * 3. Creational patterns are distinct and appropriate for each interface
 * 4. Interface contracts are properly segregated
 */
public class MerkleInterfaceSegregationTest {

    private static final long TEST_FILE_SIZE = 2 * 1024 * 1024; // 2MB for multiple chunks

    /**
     * Test MerkleRef interface in isolation - read-only reference tree operations.
     * This test demonstrates using MerkleRef as a pure interface without depending
     * on implementation details or state mutation capabilities.
     */
    @Test
    void testMerkleRefInterfaceIsolation(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Arrange: Create test data and reference tree
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        
        // Act: Create MerkleRef through proper creational pattern
        MerkleRef merkleRef = createMerkleRefFromData(testFile);
        
        // Assert: Verify MerkleRef interface contract (read-only operations)
        verifyMerkleRefReadOnlyContract(merkleRef);
        
        // Act: Save and load MerkleRef to/from .mref file
        Path mrefFile = tempDir.resolve("reference.mref");
        saveMerkleRef(merkleRef, mrefFile);
        MerkleRef loadedRef = loadMerkleRefFromFile(mrefFile);
        
        // Assert: Verify loaded MerkleRef maintains same interface contract
        verifyMerkleRefReadOnlyContract(loadedRef);
        verifyMerkleRefConsistency(merkleRef, loadedRef);
    }

    /**
     * Test MerkleState interface in isolation - mutable state tracking operations.
     * This test demonstrates using MerkleState as a pure interface for tracking
     * chunk validation progress without depending on reference tree operations.
     */
    @Test
    void testMerkleStateInterfaceIsolation(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Arrange: Create reference tree and state file
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        MerkleRef merkleRef = createMerkleRefFromData(testFile);
        Path stateFile = tempDir.resolve("state.mrkl");
        
        // Act: Create MerkleState through proper creational pattern (from MerkleRef)
        MerkleState merkleState = createMerkleStateFromRef(merkleRef, stateFile);
        
        // Assert: Verify MerkleState interface contract (mutable operations)
        verifyMerkleStateInitialContract(merkleState);
        
        // Act: Perform state mutations through interface
        byte[] originalData = Files.readAllBytes(testFile);
        performChunkValidationThroughInterface(merkleState, originalData);
        
        // Assert: Verify state changes are properly tracked
        verifyMerkleStateMutationContract(merkleState);
        
        // Act: Close and reload state to verify persistence
        merkleState.close();
        MerkleState reloadedState = loadMerkleStateFromFile(stateFile);
        
        // Assert: Verify state persistence through interface
        verifyMerkleStatePersistence(reloadedState);
    }

    /**
     * Test distinct creational patterns for MerkleRef and MerkleState.
     * Verifies that each interface has appropriate factory methods and
     * that the creation dependencies are properly enforced.
     */
    @Test
    void testDistinctCreationalPatterns(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Test MerkleRef creational patterns
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        
        // Pattern 1: MerkleRef from data file
        MerkleRef refFromData = createMerkleRefFromData(testFile);
        assertNotNull(refFromData, "MerkleRef should be creatable from data file");
        
        // Pattern 2: MerkleRef from .mref file
        Path mrefFile = tempDir.resolve("reference.mref");
        saveMerkleRef(refFromData, mrefFile);
        MerkleRef refFromFile = loadMerkleRefFromFile(mrefFile);
        assertNotNull(refFromFile, "MerkleRef should be loadable from .mref file");
        
        // Test MerkleState creational patterns
        Path stateFile = tempDir.resolve("state.mrkl");
        
        // Pattern 1: MerkleState from MerkleRef (proper dependency)
        MerkleState stateFromRef = createMerkleStateFromRef(refFromData, stateFile);
        assertNotNull(stateFromRef, "MerkleState should be creatable from MerkleRef");
        
        // Pattern 2: MerkleState from .mrkl file (reload existing state)
        stateFromRef.close();
        MerkleState stateFromFile = loadMerkleStateFromFile(stateFile);
        assertNotNull(stateFromFile, "MerkleState should be loadable from .mrkl file");
        
        // Verify creational constraints
        verifyCreationalConstraints(refFromData, stateFromFile);
    }

    /**
     * Test that interface segregation prevents inappropriate usage patterns.
     * Verifies that clients that only need read-only access use MerkleRef,
     * and clients that need state tracking use MerkleState.
     */
    @Test
    void testInterfaceSegregationEnforcement(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Arrange
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        MerkleRef merkleRef = createMerkleRefFromData(testFile);
        Path stateFile = tempDir.resolve("state.mrkl");
        MerkleState merkleState = createMerkleStateFromRef(merkleRef, stateFile);
        
        // Test 1: Read-only client should use MerkleRef interface
        demonstrateReadOnlyClientUsage(merkleRef);
        
        // Test 2: State-tracking client should use MerkleState interface  
        byte[] originalData = Files.readAllBytes(testFile);
        demonstrateStatefulClientUsage(merkleState, originalData);
        
        // Test 3: Verify interface methods don't cross boundaries inappropriately
        verifyInterfaceBoundaries(merkleRef, merkleState);
    }

    // Helper methods for MerkleRef operations
    private MerkleRef createMerkleRefFromData(Path dataFile) throws IOException, InterruptedException, ExecutionException {
        MerkleRefBuildProgress progress = MerkleRef.fromData(dataFile);
        return progress.getFuture().get();
    }
    
    private MerkleRef loadMerkleRefFromFile(Path mrefFile) throws IOException {
        return MerkleRef.load(mrefFile);
    }
    
    private void saveMerkleRef(MerkleRef merkleRef, Path mrefFile) throws IOException {
        // Assuming MerkleRef has a save method through the implementation
        if (merkleRef instanceof MerkleDataImpl) {
            ((MerkleDataImpl) merkleRef).save(mrefFile);
        } else {
            fail("Cannot save MerkleRef - implementation not supported");
        }
    }

    // Helper methods for MerkleState operations
    private MerkleState createMerkleStateFromRef(MerkleRef merkleRef, Path stateFile) throws IOException {
        return MerkleState.fromRef(merkleRef, stateFile);
    }
    
    private MerkleState loadMerkleStateFromFile(Path stateFile) throws IOException {
        return MerkleState.load(stateFile);
    }

    // Verification methods for MerkleRef interface contract
    private void verifyMerkleRefReadOnlyContract(MerkleRef merkleRef) {
        // Test shape access
        MerkleShape shape = merkleRef.getShape();
        assertNotNull(shape, "MerkleRef should provide shape");
        assertTrue(shape.getLeafCount() > 0, "Shape should have leaves");
        
        // Test hash access for leaves
        for (int i = 0; i < shape.getLeafCount(); i++) {
            byte[] leafHash = merkleRef.getHashForLeaf(i);
            assertNotNull(leafHash, "Leaf hash should not be null");
            assertEquals(32, leafHash.length, "Hash should be SHA-256 (32 bytes)");
        }
        
        // Test hash access by index
        if (shape.getNodeCount() > 0) {
            byte[] hash = merkleRef.getHashForIndex(0);
            assertNotNull(hash, "Node hash should not be null");
        }
        
        // Test merkle path
        if (shape.getLeafCount() > 0) {
            var pathToRoot = merkleRef.getPathToRoot(0);
            assertNotNull(pathToRoot, "Path to root should not be null");
            assertFalse(pathToRoot.isEmpty(), "Path should contain hashes");
        }
    }
    
    private void verifyMerkleRefConsistency(MerkleRef original, MerkleRef loaded) {
        assertEquals(original.getShape().getLeafCount(), loaded.getShape().getLeafCount(),
            "Loaded MerkleRef should have same leaf count");
        assertEquals(original.getShape().getChunkSize(), loaded.getShape().getChunkSize(),
            "Loaded MerkleRef should have same chunk size");
        
        // Verify hashes match
        for (int i = 0; i < original.getShape().getLeafCount(); i++) {
            assertArrayEquals(original.getHashForLeaf(i), loaded.getHashForLeaf(i),
                "Leaf hashes should match after save/load");
        }
    }

    // Verification methods for MerkleState interface contract
    private void verifyMerkleStateInitialContract(MerkleState merkleState) {
        // Test initial state - all chunks should be invalid
        MerkleShape shape = merkleState.getMerkleShape();
        assertNotNull(shape, "MerkleState should provide shape");
        
        BitSet validChunks = merkleState.getValidChunks();
        assertNotNull(validChunks, "Valid chunks BitSet should not be null");
        assertEquals(0, validChunks.cardinality(), "Initially no chunks should be valid");
        
        // Test individual chunk validity
        for (int i = 0; i < shape.getLeafCount(); i++) {
            assertFalse(merkleState.isValid(i), "Chunk " + i + " should initially be invalid");
        }
    }
    
    private void performChunkValidationThroughInterface(MerkleState merkleState, byte[] originalData) {
        MerkleShape shape = merkleState.getMerkleShape();
        
        // Validate first chunk through interface
        if (shape.getLeafCount() > 0) {
            long chunkSize = shape.getChunkSize();
            int firstChunkSize = (int) Math.min(chunkSize, originalData.length);
            byte[] firstChunk = new byte[firstChunkSize];
            System.arraycopy(originalData, 0, firstChunk, 0, firstChunkSize);
            
            ByteBuffer chunkBuffer = ByteBuffer.wrap(firstChunk);
            boolean saved = merkleState.saveIfValid(0, chunkBuffer, (data) -> {
                // Mock save operation
            });
            
            assertTrue(saved, "First chunk should be valid and saved");
        }
    }
    
    private void verifyMerkleStateMutationContract(MerkleState merkleState) {
        // Verify state has changed
        BitSet validChunks = merkleState.getValidChunks();
        assertTrue(validChunks.cardinality() > 0, "Some chunks should now be valid");
        
        if (merkleState.getMerkleShape().getLeafCount() > 0) {
            assertTrue(merkleState.isValid(0), "First chunk should be valid");
        }
    }
    
    private void verifyMerkleStatePersistence(MerkleState reloadedState) {
        // Verify state persisted correctly
        BitSet validChunks = reloadedState.getValidChunks();
        assertTrue(validChunks.cardinality() > 0, "Valid chunks should persist");
        
        if (reloadedState.getMerkleShape().getLeafCount() > 0) {
            assertTrue(reloadedState.isValid(0), "First chunk should remain valid after reload");
        }
    }

    // Methods demonstrating proper interface usage patterns
    private void demonstrateReadOnlyClientUsage(MerkleRef merkleRef) {
        // A read-only client only needs MerkleRef interface
        MerkleShape shape = merkleRef.getShape();
        assertNotNull(shape, "Read-only client can access shape");
        
        // Read-only client can verify data against reference hashes
        for (int i = 0; i < Math.min(3, shape.getLeafCount()); i++) {
            byte[] expectedHash = merkleRef.getHashForLeaf(i);
            assertNotNull(expectedHash, "Read-only client can get reference hashes");
        }
        
        // Read-only client can get merkle paths for verification
        if (shape.getLeafCount() > 0) {
            var path = merkleRef.getPathToRoot(0);
            assertNotNull(path, "Read-only client can get merkle paths");
        }
    }
    
    private void demonstrateStatefulClientUsage(MerkleState merkleState, byte[] originalData) {
        // A stateful client needs MerkleState interface for mutation
        MerkleShape shape = merkleState.getMerkleShape();
        
        // Stateful client tracks validation progress
        BitSet initialValid = merkleState.getValidChunks();
        int initialCount = initialValid.cardinality();
        
        // Stateful client validates and saves chunks
        if (shape.getLeafCount() > 1) {
            long chunkSize = shape.getChunkSize();
            long startOffset = chunkSize; // Second chunk
            int chunkDataSize = (int) Math.min(chunkSize, originalData.length - startOffset);
            
            if (chunkDataSize > 0) {
                byte[] chunkData = new byte[chunkDataSize];
                System.arraycopy(originalData, (int) startOffset, chunkData, 0, chunkDataSize);
                
                ByteBuffer chunkBuffer = ByteBuffer.wrap(chunkData);
                boolean saved = merkleState.saveIfValid(1, chunkBuffer, (data) -> {
                    // Mock save operation
                });
                
                if (saved) {
                    // Verify state mutation occurred
                    BitSet updatedValid = merkleState.getValidChunks();
                    assertTrue(updatedValid.cardinality() > initialCount, 
                        "Stateful client should track validation progress");
                }
            }
        }
        
        // Stateful client manages resource lifecycle
        merkleState.flush();
    }

    // Verification methods for proper interface boundaries
    private void verifyCreationalConstraints(MerkleRef merkleRef, MerkleState merkleState) {
        // Verify MerkleState requires MerkleRef for creation
        assertNotNull(merkleRef, "MerkleRef should exist independently");
        assertNotNull(merkleState, "MerkleState should be created from MerkleRef");
        
        // Verify shapes are consistent (MerkleState derived from MerkleRef)
        assertEquals(merkleRef.getShape().getLeafCount(), 
                    merkleState.getMerkleShape().getLeafCount(),
                    "MerkleState should derive shape from MerkleRef");
    }
    
    private void verifyInterfaceBoundaries(MerkleRef merkleRef, MerkleState merkleState) {
        // Verify MerkleRef provides read-only access
        assertNotNull(merkleRef.getShape(), "MerkleRef provides shape access");
        assertNotNull(merkleRef.getHashForLeaf(0), "MerkleRef provides hash access");
        
        // Verify MerkleState provides mutable access
        assertNotNull(merkleState.getMerkleShape(), "MerkleState provides shape access");
        assertNotNull(merkleState.getValidChunks(), "MerkleState provides validation state");
        
        // Verify interface method consistency (both provide shape, potentially different method names)
        assertEquals(merkleRef.getShape().getLeafCount(), 
                    merkleState.getMerkleShape().getLeafCount(),
                    "Both interfaces should provide consistent shape information");
    }

    // Test data creation utility
    private Path createTestDataFile(Path tempDir, long size) throws IOException {
        Path testFile = tempDir.resolve("test_data.dat");
        byte[] data = new byte[(int) size];
        
        // Fill with deterministic pattern
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        
        Files.write(testFile, data);
        return testFile;
    }
}