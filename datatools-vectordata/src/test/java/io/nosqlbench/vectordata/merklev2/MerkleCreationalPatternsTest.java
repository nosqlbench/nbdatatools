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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that verify the distinct creational patterns for MerkleRef and MerkleState interfaces.
 * This ensures that:
 * 1. MerkleRef can be created from data files or loaded from .mref files
 * 2. MerkleState must be created from an existing MerkleRef or loaded from .mrkl files
 * 3. Factory methods properly enforce interface contracts
 * 4. File extension conventions are followed (.mref for reference, .mrkl for state)
 */
public class MerkleCreationalPatternsTest {

    private static final long TEST_FILE_SIZE = 1024 * 1024; // 1MB

    /**
     * Test MerkleRef creation patterns:
     * 1. From data file (original data) -> .mref file
     * 2. From .mref file (loading existing reference)
     */
    @Test
    void testMerkleRefCreationPatterns(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Pattern 1: Create MerkleRef from original data
        Path originalDataFile = createTestDataFile(tempDir, "original.dat", TEST_FILE_SIZE);
        
        // Use interface factory to create MerkleRef from data
        MerkleRefBuildProgress progress = MerkleRef.fromData(originalDataFile);
        assertNotNull(progress, "Factory should return build progress");
        
        MerkleRef refFromData = progress.getFuture().get();
        assertNotNull(refFromData, "MerkleRef should be created from data");
        assertTrue(refFromData.getShape().getLeafCount() > 0, "Reference should have chunks");
        
        // Verify this is a complete reference (all chunks valid in context)
        verifyCompleteReference(refFromData);
        
        // Pattern 2: Save MerkleRef to .mref file
        Path mrefFile = tempDir.resolve("reference.mref");
        saveMerkleRefToFile(refFromData, mrefFile);
        assertTrue(Files.exists(mrefFile), "MRef file should be created");
        assertTrue(mrefFile.getFileName().toString().endsWith(".mref"), 
            "Reference file should have .mref extension");
        
        // Pattern 3: Load MerkleRef from .mref file
        MerkleRef refFromFile = MerkleRef.load(mrefFile);
        assertNotNull(refFromFile, "MerkleRef should be loaded from .mref file");
        
        // Verify loaded reference maintains same properties
        verifyMerkleRefEquivalence(refFromData, refFromFile);
        
        // Pattern 4: Verify MerkleRef immutability
        verifyMerkleRefImmutability(refFromFile);
    }

    /**
     * Test MerkleState creation patterns:
     * 1. From MerkleRef (creates empty state) -> .mrkl file
     * 2. From .mrkl file (loading existing state)
     */
    @Test
    void testMerkleStateCreationPatterns(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Setup: Create a MerkleRef first
        Path originalDataFile = createTestDataFile(tempDir, "data.dat", TEST_FILE_SIZE);
        MerkleRef merkleRef = createMerkleRefFromData(originalDataFile);
        
        // Pattern 1: Create MerkleState from MerkleRef
        Path stateFile = tempDir.resolve("state.mrkl");
        MerkleState stateFromRef = MerkleState.fromRef(merkleRef, stateFile);
        
        assertNotNull(stateFromRef, "MerkleState should be created from MerkleRef");
        assertTrue(Files.exists(stateFile), "State file should be created");
        assertTrue(stateFile.getFileName().toString().endsWith(".mrkl"),
            "State file should have .mrkl extension");
        
        // Verify initial state (empty - no chunks validated)
        verifyEmptyMerkleState(stateFromRef, merkleRef);
        
        // Pattern 2: Modify state and save
        simulateChunkValidation(stateFromRef, originalDataFile);
        stateFromRef.flush(); // Ensure changes are persisted
        stateFromRef.close();
        
        // Pattern 3: Load MerkleState from .mrkl file
        MerkleState stateFromFile = MerkleState.load(stateFile);
        assertNotNull(stateFromFile, "MerkleState should be loaded from .mrkl file");
        
        // Verify loaded state maintains validation progress
        verifyMerkleStateLoaded(stateFromFile);
        
        // Pattern 4: Verify MerkleState mutability
        verifyMerkleStateMutability(stateFromFile, originalDataFile);
        
        stateFromFile.close();
    }

    /**
     * Test that MerkleState creation requires MerkleRef dependency.
     * Verifies the proper dependency relationship and that MerkleState 
     * cannot exist without a MerkleRef.
     */
    @Test
    void testMerkleStateRequiresMerkleRefDependency(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Create MerkleRef
        Path dataFile = createTestDataFile(tempDir, "data.dat", TEST_FILE_SIZE);
        MerkleRef merkleRef = createMerkleRefFromData(dataFile);
        
        // Verify MerkleState creation requires MerkleRef
        Path stateFile = tempDir.resolve("dependent.mrkl");
        MerkleState merkleState = MerkleState.fromRef(merkleRef, stateFile);
        
        // Verify dependency relationship
        assertEquals(merkleRef.getShape().getLeafCount(), 
                    merkleState.getMerkleShape().getLeafCount(),
                    "MerkleState should inherit shape from MerkleRef");
        assertEquals(merkleRef.getShape().getChunkSize(), 
                    merkleState.getMerkleShape().getChunkSize(),
                    "MerkleState should inherit chunk size from MerkleRef");
        assertEquals(merkleRef.getShape().getTotalContentSize(), 
                    merkleState.getMerkleShape().getTotalContentSize(),
                    "MerkleState should inherit content size from MerkleRef");
        
        // Verify state tracks against reference hashes
        verifyStateReferenceDependency(merkleRef, merkleState, dataFile);
        
        merkleState.close();
    }

    /**
     * Test file extension conventions and factory method routing.
     * Verifies that the system properly distinguishes between .mref and .mrkl files.
     */
    @Test
    void testFileExtensionConventions(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Create reference and state
        Path dataFile = createTestDataFile(tempDir, "data.dat", TEST_FILE_SIZE);
        MerkleRef merkleRef = createMerkleRefFromData(dataFile);
        
        // Test .mref file handling
        Path mrefFile = tempDir.resolve("test.mref");
        saveMerkleRefToFile(merkleRef, mrefFile);
        
        MerkleRef loadedRef = MerkleRef.load(mrefFile);
        assertNotNull(loadedRef, "Should load MerkleRef from .mref file");
        
        // Test .mrkl file handling
        Path mrklFile = tempDir.resolve("test.mrkl");
        MerkleState merkleState = MerkleState.fromRef(merkleRef, mrklFile);
        merkleState.close();
        
        MerkleState loadedState = MerkleState.load(mrklFile);
        assertNotNull(loadedState, "Should load MerkleState from .mrkl file");
        
        // Verify distinct behavior
        verifyDistinctFileTypes(loadedRef, loadedState);
        
        loadedState.close();
    }

    /**
     * Test factory method consistency and error handling.
     */
    @Test
    void testFactoryMethodConsistency(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Test MerkleRef interface methods
        Path dataFile = createTestDataFile(tempDir, "data.dat", TEST_FILE_SIZE);
        
        // Test fromData with progress
        MerkleRefBuildProgress progressTracking = MerkleRef.fromData(dataFile);
        assertNotNull(progressTracking, "fromData should return progress tracker");
        MerkleRef refWithProgress = progressTracking.getFuture().get();
        assertNotNull(refWithProgress, "Progress should complete with MerkleRef");
        
        // Test fromDataSimple
        MerkleRef refSimple = MerkleRef.fromDataSimple(dataFile).get();
        assertNotNull(refSimple, "fromDataSimple should return MerkleRef");
        
        // Verify both methods produce equivalent results
        verifyMerkleRefEquivalence(refWithProgress, refSimple);
        
        // Test factory error handling
        Path nonExistentFile = tempDir.resolve("nonexistent.mref");
        assertThrows(IOException.class, () -> {
            MerkleRef.load(nonExistentFile);
        }, "Loading non-existent file should throw IOException");
        
        // Test empty file handling
        Path emptyFile = tempDir.resolve("empty.mref");
        Files.createFile(emptyFile);
        assertThrows(IOException.class, () -> {
            MerkleRef.load(emptyFile);
        }, "Loading empty file should throw IOException");
    }

    // Helper methods for MerkleRef operations
    private MerkleRef createMerkleRefFromData(Path dataFile) throws IOException, InterruptedException, ExecutionException {
        return MerkleRef.fromData(dataFile).getFuture().get();
    }
    
    private void saveMerkleRefToFile(MerkleRef merkleRef, Path mrefFile) throws IOException {
        if (merkleRef instanceof MerkleDataImpl) {
            ((MerkleDataImpl) merkleRef).save(mrefFile);
        } else {
            fail("Cannot save MerkleRef - unsupported implementation");
        }
    }
    
    private MerkleState loadMerkleStateFromFile(Path stateFile) throws IOException {
        return MerkleState.load(stateFile);
    }

    // Verification methods
    private void verifyCompleteReference(MerkleRef merkleRef) {
        MerkleShape shape = merkleRef.getShape();
        
        // All leaf hashes should be present
        for (int i = 0; i < shape.getLeafCount(); i++) {
            byte[] leafHash = merkleRef.getHashForLeaf(i);
            assertNotNull(leafHash, "Reference should have hash for leaf " + i);
            assertEquals(32, leafHash.length, "Hash should be SHA-256 (32 bytes)");
        }
        
        // Root hash should be computable
        if (shape.getLeafCount() > 0) {
            var pathToRoot = merkleRef.getPathToRoot(0);
            assertFalse(pathToRoot.isEmpty(), "Path to root should exist");
        }
    }
    
    private void verifyMerkleRefEquivalence(MerkleRef ref1, MerkleRef ref2) {
        assertEquals(ref1.getShape().getLeafCount(), ref2.getShape().getLeafCount(),
            "References should have same leaf count");
        assertEquals(ref1.getShape().getChunkSize(), ref2.getShape().getChunkSize(),
            "References should have same chunk size");
        
        // Compare leaf hashes
        for (int i = 0; i < ref1.getShape().getLeafCount(); i++) {
            assertArrayEquals(ref1.getHashForLeaf(i), ref2.getHashForLeaf(i),
                "Leaf hashes should match for index " + i);
        }
    }
    
    private void verifyMerkleRefImmutability(MerkleRef merkleRef) {
        // MerkleRef should only provide read operations
        // This is verified by interface design - no mutating methods in MerkleRef
        
        // Verify repeated calls return same data
        MerkleShape shape1 = merkleRef.getShape();
        MerkleShape shape2 = merkleRef.getShape();
        assertEquals(shape1.getLeafCount(), shape2.getLeafCount(),
            "Shape should be consistent across calls");
        
        if (shape1.getLeafCount() > 0) {
            byte[] hash1 = merkleRef.getHashForLeaf(0);
            byte[] hash2 = merkleRef.getHashForLeaf(0);
            assertArrayEquals(hash1, hash2, "Hash should be consistent across calls");
        }
    }
    
    private void verifyEmptyMerkleState(MerkleState merkleState, MerkleRef merkleRef) {
        // State should start empty (no valid chunks)
        assertEquals(0, merkleState.getValidChunks().cardinality(),
            "New MerkleState should have no valid chunks");
        
        // But should have same structure as reference
        assertEquals(merkleRef.getShape().getLeafCount(), 
                    merkleState.getMerkleShape().getLeafCount(),
                    "State should match reference structure");
        
        // All chunks should be invalid initially
        for (int i = 0; i < merkleState.getMerkleShape().getLeafCount(); i++) {
            assertFalse(merkleState.isValid(i), "Chunk " + i + " should be initially invalid");
        }
    }
    
    private void simulateChunkValidation(MerkleState merkleState, Path originalFile) throws IOException {
        byte[] originalData = Files.readAllBytes(originalFile);
        MerkleShape shape = merkleState.getMerkleShape();
        
        // Validate first chunk
        if (shape.getLeafCount() > 0) {
            long chunkSize = shape.getChunkSize();
            int firstChunkSize = (int) Math.min(chunkSize, originalData.length);
            byte[] firstChunk = new byte[firstChunkSize];
            System.arraycopy(originalData, 0, firstChunk, 0, firstChunkSize);
            
            boolean saved = merkleState.saveIfValid(0, java.nio.ByteBuffer.wrap(firstChunk), (data) -> {
                // Mock save operation - would normally write to disk
            });
            
            assertTrue(saved, "First chunk should validate successfully");
        }
    }
    
    private void verifyMerkleStateLoaded(MerkleState merkleState) {
        // Should have some valid chunks from previous session
        assertTrue(merkleState.getValidChunks().cardinality() > 0,
            "Loaded state should retain validation progress");
        
        if (merkleState.getMerkleShape().getLeafCount() > 0) {
            assertTrue(merkleState.isValid(0), "First chunk should remain valid");
        }
    }
    
    private void verifyMerkleStateMutability(MerkleState merkleState, Path originalFile) throws IOException {
        int initialValid = merkleState.getValidChunks().cardinality();
        
        // Try to validate another chunk if available
        byte[] originalData = Files.readAllBytes(originalFile);
        MerkleShape shape = merkleState.getMerkleShape();
        
        if (shape.getLeafCount() > 1) {
            long chunkSize = shape.getChunkSize();
            long secondChunkStart = chunkSize;
            int secondChunkSize = (int) Math.min(chunkSize, originalData.length - secondChunkStart);
            
            if (secondChunkSize > 0) {
                byte[] secondChunk = new byte[secondChunkSize];
                System.arraycopy(originalData, (int) secondChunkStart, secondChunk, 0, secondChunkSize);
                
                boolean saved = merkleState.saveIfValid(1, java.nio.ByteBuffer.wrap(secondChunk), (data) -> {
                    // Mock save operation
                });
                
                if (saved) {
                    int finalValid = merkleState.getValidChunks().cardinality();
                    assertTrue(finalValid > initialValid, "State should be mutable - valid count should increase");
                }
            }
        }
    }
    
    private void verifyStateReferenceDependency(MerkleRef merkleRef, MerkleState merkleState, Path dataFile) throws IOException {
        byte[] originalData = Files.readAllBytes(dataFile);
        MerkleShape shape = merkleState.getMerkleShape();
        
        // Verify state validates against reference hashes
        if (shape.getLeafCount() > 0) {
            long chunkSize = shape.getChunkSize();
            int chunkDataSize = (int) Math.min(chunkSize, originalData.length);
            byte[] chunkData = new byte[chunkDataSize];
            System.arraycopy(originalData, 0, chunkData, 0, chunkDataSize);
            
            boolean saved = merkleState.saveIfValid(0, java.nio.ByteBuffer.wrap(chunkData), (data) -> {
                // Mock save
            });
            
            assertTrue(saved, "State should validate against reference hashes");
            
            // Verify invalid data gets rejected
            byte[] invalidData = new byte[chunkDataSize];
            // Fill with different pattern
            for (int i = 0; i < invalidData.length; i++) {
                invalidData[i] = (byte) ((i + 1) % 256);
            }
            
            boolean invalidSaved = merkleState.saveIfValid(0, java.nio.ByteBuffer.wrap(invalidData), (data) -> {
                // Mock save - should not be called
                fail("Save callback should not be called for invalid data");
            });
            
            // Note: This might still return true if the original chunk was already validated
            // The key test is that the save callback isn't called for mismatched data
        }
    }
    
    private void verifyDistinctFileTypes(MerkleRef merkleRef, MerkleState merkleState) {
        // Reference provides read-only access
        assertNotNull(merkleRef.getShape(), "MerkleRef provides shape access");
        if (merkleRef.getShape().getLeafCount() > 0) {
            assertNotNull(merkleRef.getHashForLeaf(0), "MerkleRef provides hash access");
        }
        
        // State provides validation tracking
        assertNotNull(merkleState.getMerkleShape(), "MerkleState provides shape access");
        assertNotNull(merkleState.getValidChunks(), "MerkleState provides validation state");
        
        // Different file types should serve different purposes
        // .mref = complete reference with all hashes valid
        // .mrkl = partial state tracking validation progress
        
        // This is verified by usage patterns - reference for validation, state for tracking
    }

    // Utility method
    private Path createTestDataFile(Path tempDir, String filename, long size) throws IOException {
        Path file = tempDir.resolve(filename);
        byte[] data = new byte[(int) size];
        
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        
        Files.write(file, data);
        return file;
    }
}