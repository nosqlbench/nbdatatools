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
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for converting MerkleState to MerkleRef when all chunks are validated.
/// This test class verifies:
/// 1. Successful conversion when all chunks are valid
/// 2. Exception thrown when attempting conversion with incomplete validation
/// 3. Proper interface behavior after conversion
/// 4. Edge cases and error conditions
public class MerkleStateToRefConversionTest {

    private static final long TEST_FILE_SIZE = 2 * 1024 * 1024; // 2MB for multiple chunks

    /// Test successful conversion of fully validated MerkleState to MerkleRef
    @Test
    void testCompleteStateToRefConversion(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Arrange: Create test data and fully validate all chunks
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        byte[] originalData = Files.readAllBytes(testFile);
        
        // Create MerkleRef and MerkleState
        MerkleRef originalRef = MerkleRef.fromDataSimple(testFile).get();
        Path stateFile = tempDir.resolve("complete.mrkl");
        MerkleState merkleState = MerkleState.fromRef(originalRef, stateFile);
        
        // Validate all chunks
        validateAllChunks(merkleState, originalData);
        
        // Act: Convert to MerkleRef
        MerkleRef convertedRef = merkleState.toRef();
        
        // Assert: Verify successful conversion
        assertNotNull(convertedRef, "Conversion should return a MerkleRef");
        assertSame(merkleState, convertedRef, "Converted ref should be the same object (different interface view)");
        
        // Verify MerkleRef interface works correctly
        verifyMerkleRefInterface(convertedRef, originalRef);
        
        merkleState.close();
    }

    /// Test that conversion fails when MerkleState is incomplete
    @Test
    void testIncompleteStateToRefConversionFails(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Arrange: Create test data and partially validate chunks
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        byte[] originalData = Files.readAllBytes(testFile);
        
        // Create MerkleRef and MerkleState
        MerkleRef originalRef = MerkleRef.fromDataSimple(testFile).get();
        Path stateFile = tempDir.resolve("incomplete.mrkl");
        MerkleState merkleState = MerkleState.fromRef(originalRef, stateFile);
        
        // Validate only some chunks (not all)
        MerkleShape shape = merkleState.getMerkleShape();
        int totalChunks = shape.getLeafCount();
        int chunksToValidate = Math.max(1, totalChunks - 1); // Leave at least one chunk unvalidated
        
        validateSomeChunks(merkleState, originalData, chunksToValidate);
        
        // Act & Assert: Attempt conversion should fail
        IncompleteMerkleStateException exception = assertThrows(
            IncompleteMerkleStateException.class,
            () -> merkleState.toRef(),
            "Conversion should fail with incomplete state"
        );
        
        // Verify exception details
        assertEquals(chunksToValidate, exception.getValidChunkCount(), 
            "Exception should report correct valid chunk count");
        assertEquals(totalChunks, exception.getTotalChunkCount(),
            "Exception should report correct total chunk count");
        
        String expectedMessage = String.format("Cannot create MerkleRef from incomplete MerkleState. Only %d of %d chunks are valid.", 
            chunksToValidate, totalChunks);
        assertEquals(expectedMessage, exception.getMessage(),
            "Exception should have descriptive message");
        
        merkleState.close();
    }

    /// Test conversion with empty state (zero chunks validated)
    @Test
    void testEmptyStateToRefConversionFails(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Arrange: Create MerkleState with no validated chunks
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        
        MerkleRef originalRef = MerkleRef.fromDataSimple(testFile).get();
        Path stateFile = tempDir.resolve("empty.mrkl");
        MerkleState merkleState = MerkleState.fromRef(originalRef, stateFile);
        
        // Don't validate any chunks - state remains empty
        int totalChunks = merkleState.getMerkleShape().getLeafCount();
        
        // Act & Assert: Conversion should fail
        IncompleteMerkleStateException exception = assertThrows(
            IncompleteMerkleStateException.class,
            () -> merkleState.toRef(),
            "Conversion should fail with empty state"
        );
        
        assertEquals(0, exception.getValidChunkCount(), "Should have zero valid chunks");
        assertEquals(totalChunks, exception.getTotalChunkCount(), "Should report correct total");
        
        merkleState.close();
    }

    /// Test conversion with single chunk file
    @Test
    void testSingleChunkStateToRefConversion(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Arrange: Create small file that results in single chunk
        long smallFileSize = 32 * 1024; // 32KB - should result in single chunk
        Path testFile = createTestDataFile(tempDir, smallFileSize);
        byte[] originalData = Files.readAllBytes(testFile);
        
        MerkleRef originalRef = MerkleRef.fromDataSimple(testFile).get();
        Path stateFile = tempDir.resolve("single.mrkl");
        MerkleState merkleState = MerkleState.fromRef(originalRef, stateFile);
        
        // Verify we have exactly one chunk
        assertEquals(1, merkleState.getMerkleShape().getLeafCount(),
            "Test should use single chunk file");
        
        // Validate the single chunk
        validateAllChunks(merkleState, originalData);
        
        // Act: Convert to MerkleRef
        MerkleRef convertedRef = merkleState.toRef();
        
        // Assert: Verify successful conversion
        assertNotNull(convertedRef, "Single chunk conversion should succeed");
        assertEquals(1, convertedRef.getShape().getLeafCount(),
            "Converted ref should have single chunk");
        
        merkleState.close();
    }

    /// Test that conversion fails on closed state
    @Test
    void testClosedStateToRefConversionFails(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Arrange: Create and fully validate state, then close it
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        byte[] originalData = Files.readAllBytes(testFile);
        
        MerkleRef originalRef = MerkleRef.fromDataSimple(testFile).get();
        Path stateFile = tempDir.resolve("closed.mrkl");
        MerkleState merkleState = MerkleState.fromRef(originalRef, stateFile);
        
        // Validate all chunks
        validateAllChunks(merkleState, originalData);
        
        // Close the state
        merkleState.close();
        
        // Act & Assert: Attempt conversion should fail
        IllegalStateException exception = assertThrows(
            IllegalStateException.class,
            () -> merkleState.toRef(),
            "Conversion should fail on closed state"
        );
        
        assertEquals("MerkleState is closed", exception.getMessage(),
            "Exception should indicate state is closed");
    }

    /// Test that converted MerkleRef maintains proper interface contract
    @Test
    void testConvertedRefInterfaceContract(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Arrange: Create fully validated state
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        byte[] originalData = Files.readAllBytes(testFile);
        
        MerkleRef originalRef = MerkleRef.fromDataSimple(testFile).get();
        Path stateFile = tempDir.resolve("contract.mrkl");
        MerkleState merkleState = MerkleState.fromRef(originalRef, stateFile);
        
        validateAllChunks(merkleState, originalData);
        
        // Act: Convert to MerkleRef
        MerkleRef convertedRef = merkleState.toRef();
        
        // Assert: Verify MerkleRef interface methods work correctly
        MerkleShape shape = convertedRef.getShape();
        assertNotNull(shape, "Converted ref should provide shape");
        
        // Test hash access
        for (int i = 0; i < shape.getLeafCount(); i++) {
            byte[] leafHash = convertedRef.getHashForLeaf(i);
            assertNotNull(leafHash, "Converted ref should provide leaf hashes");
            assertEquals(32, leafHash.length, "Hash should be SHA-256");
            
            // Compare with original reference
            assertArrayEquals(originalRef.getHashForLeaf(i), leafHash,
                "Converted ref hashes should match original");
        }
        
        // Test merkle path
        if (shape.getLeafCount() > 0) {
            var pathToRoot = convertedRef.getPathToRoot(0);
            assertNotNull(pathToRoot, "Converted ref should provide merkle paths");
            assertFalse(pathToRoot.isEmpty(), "Path should not be empty");
        }
        
        merkleState.close();
    }

    /// Test that MerkleState interface still works after conversion
    @Test
    void testStateInterfaceAfterConversion(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Arrange: Create fully validated state
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        byte[] originalData = Files.readAllBytes(testFile);
        
        MerkleRef originalRef = MerkleRef.fromDataSimple(testFile).get();
        Path stateFile = tempDir.resolve("dual.mrkl");
        MerkleState merkleState = MerkleState.fromRef(originalRef, stateFile);
        
        validateAllChunks(merkleState, originalData);
        
        // Act: Convert to MerkleRef but keep using as MerkleState
        MerkleRef convertedRef = merkleState.toRef();
        
        // Assert: MerkleState interface should still work
        assertNotNull(merkleState.getMerkleShape(), "State interface should still work");
        assertNotNull(merkleState.getValidChunks(), "State should still provide valid chunks");
        
        // All chunks should be valid
        int totalChunks = merkleState.getMerkleShape().getLeafCount();
        assertEquals(totalChunks, merkleState.getValidChunks().cardinality(),
            "All chunks should remain valid");
        
        for (int i = 0; i < totalChunks; i++) {
            assertTrue(merkleState.isValid(i), "Chunk " + i + " should be valid");
        }
        
        merkleState.close();
    }

    /// Test persistence of converted state
    @Test
    void testConvertedStatePersistence(@TempDir Path tempDir) throws IOException, InterruptedException, ExecutionException {
        // Arrange: Create, validate, and convert state
        Path testFile = createTestDataFile(tempDir, TEST_FILE_SIZE);
        byte[] originalData = Files.readAllBytes(testFile);
        
        MerkleRef originalRef = MerkleRef.fromDataSimple(testFile).get();
        Path stateFile = tempDir.resolve("persist.mrkl");
        MerkleState merkleState = MerkleState.fromRef(originalRef, stateFile);
        
        validateAllChunks(merkleState, originalData);
        
        // Convert and close
        MerkleRef convertedRef = merkleState.toRef();
        assertNotNull(convertedRef, "Conversion should succeed");
        
        merkleState.close();
        
        // Act: Reload state from file
        MerkleState reloadedState = MerkleState.load(stateFile);
        
        // Assert: Reloaded state should be complete and convertible
        int totalChunks = reloadedState.getMerkleShape().getLeafCount();
        assertEquals(totalChunks, reloadedState.getValidChunks().cardinality(),
            "Reloaded state should be complete");
        
        MerkleRef reConvertedRef = reloadedState.toRef();
        assertNotNull(reConvertedRef, "Reloaded state should be convertible");
        
        reloadedState.close();
    }

    // Helper methods
    private Path createTestDataFile(Path tempDir, long size) throws IOException {
        Path file = tempDir.resolve("test_data.dat");
        byte[] data = new byte[(int) size];
        
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        
        Files.write(file, data);
        return file;
    }
    
    private void validateAllChunks(MerkleState merkleState, byte[] originalData) {
        MerkleShape shape = merkleState.getMerkleShape();
        long chunkSize = shape.getChunkSize();
        
        for (int chunkIndex = 0; chunkIndex < shape.getLeafCount(); chunkIndex++) {
            long startOffset = (long) chunkIndex * chunkSize;
            int dataSize = (int) Math.min(chunkSize, originalData.length - startOffset);
            
            if (dataSize > 0) {
                byte[] chunkData = new byte[dataSize];
                System.arraycopy(originalData, (int) startOffset, chunkData, 0, dataSize);
                
                ByteBuffer chunkBuffer = ByteBuffer.wrap(chunkData);
                boolean saved = merkleState.saveIfValid(chunkIndex, chunkBuffer, (data) -> {
                    // Mock save operation
                });
                
                assertTrue(saved, "Chunk " + chunkIndex + " should validate and save successfully");
            }
        }
        
        // Verify all chunks are now valid
        assertEquals(shape.getLeafCount(), merkleState.getValidChunks().cardinality(),
            "All chunks should be validated");
    }
    
    private void validateSomeChunks(MerkleState merkleState, byte[] originalData, int count) {
        MerkleShape shape = merkleState.getMerkleShape();
        long chunkSize = shape.getChunkSize();
        int chunksToValidate = Math.min(count, shape.getLeafCount());
        
        for (int chunkIndex = 0; chunkIndex < chunksToValidate; chunkIndex++) {
            long startOffset = (long) chunkIndex * chunkSize;
            int dataSize = (int) Math.min(chunkSize, originalData.length - startOffset);
            
            if (dataSize > 0) {
                byte[] chunkData = new byte[dataSize];
                System.arraycopy(originalData, (int) startOffset, chunkData, 0, dataSize);
                
                ByteBuffer chunkBuffer = ByteBuffer.wrap(chunkData);
                boolean saved = merkleState.saveIfValid(chunkIndex, chunkBuffer, (data) -> {
                    // Mock save operation
                });
                
                assertTrue(saved, "Chunk " + chunkIndex + " should validate and save successfully");
            }
        }
        
        // Verify only the expected chunks are valid
        assertEquals(chunksToValidate, merkleState.getValidChunks().cardinality(),
            "Only " + chunksToValidate + " chunks should be validated");
    }
    
    private void verifyMerkleRefInterface(MerkleRef convertedRef, MerkleRef originalRef) {
        // Compare shapes
        assertEquals(originalRef.getShape().getLeafCount(), convertedRef.getShape().getLeafCount(),
            "Leaf counts should match");
        assertEquals(originalRef.getShape().getChunkSize(), convertedRef.getShape().getChunkSize(),
            "Chunk sizes should match");
        assertEquals(originalRef.getShape().getTotalContentSize(), convertedRef.getShape().getTotalContentSize(),
            "Content sizes should match");
        
        // Compare leaf hashes
        for (int i = 0; i < originalRef.getShape().getLeafCount(); i++) {
            assertArrayEquals(originalRef.getHashForLeaf(i), convertedRef.getHashForLeaf(i),
                "Leaf hash " + i + " should match");
        }
        
        // Compare node hashes
        for (int i = 0; i < originalRef.getShape().getNodeCount(); i++) {
            assertArrayEquals(originalRef.getHashForIndex(i), convertedRef.getHashForIndex(i),
                "Node hash " + i + " should match");
        }
    }
}