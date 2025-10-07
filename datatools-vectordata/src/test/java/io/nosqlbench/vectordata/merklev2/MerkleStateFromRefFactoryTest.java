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
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for the new MerkleDataImpl.createStateFromRef factory method.
 * This test verifies that the factory method correctly creates a MerkleState
 * with all valid bits cleared (set to false).
 */
public class MerkleStateFromRefFactoryTest {

    @TempDir
    Path tempDir;

    @Test
    public void testCreateStateFromRef() throws IOException, ExecutionException, InterruptedException {
        // Create test data
        Path testFile = tempDir.resolve("test_data.bin");
        byte[] data = new byte[2 * 1024 * 1024]; // 2MB file
        Random random = new Random(12345);
        random.nextBytes(data);
        Files.write(testFile, data);

        // Create MerkleRef from the test data
        MerkleRef merkleRef = MerkleRefFactory.fromDataSimple(testFile).get();
        
        // Use the new factory method to create MerkleState
        Path statePath = tempDir.resolve("test_state.mrkl");
        MerkleState merkleState = MerkleDataImpl.createStateFromRef(merkleRef, statePath);

        try {
            // Verify the state file was created
            assertTrue(Files.exists(statePath), "State file should be created");
            
            // Verify the shape matches the reference
            assertEquals(merkleRef.getShape().getLeafCount(), 
                        merkleState.getMerkleShape().getLeafCount(),
                        "State should have same leaf count as reference");
            assertEquals(merkleRef.getShape().getChunkSize(), 
                        merkleState.getMerkleShape().getChunkSize(),
                        "State should have same chunk size as reference");
            
            // Verify ALL valid bits are cleared (most important test)
            BitSet validChunks = merkleState.getValidChunks();
            assertEquals(0, validChunks.cardinality(), 
                        "All valid bits should be cleared initially");
            
            // Verify each individual chunk is marked as invalid
            MerkleShape shape = merkleState.getMerkleShape();
            for (int i = 0; i < shape.getLeafCount(); i++) {
                assertFalse(merkleState.isValid(i), 
                          "Chunk " + i + " should be initially invalid");
            }
            
            // Verify we can validate chunks (the hashes should be available)
            // Read some actual data and try to validate it
            byte[] chunkData = new byte[(int) shape.getChunkSize()];
            System.arraycopy(data, 0, chunkData, 0, Math.min(chunkData.length, data.length));
            
            // This should succeed because we have the correct hash from the reference
            boolean isValid = merkleState.saveIfValid(0, java.nio.ByteBuffer.wrap(chunkData), 
                buffer -> { /* No-op save callback for test */ });
            assertTrue(isValid, "Should be able to validate chunk with correct data");
            
            // After validation, chunk 0 should be marked as valid
            assertTrue(merkleState.isValid(0), "Chunk 0 should be valid after validation");
            assertEquals(1, merkleState.getValidChunks().cardinality(), 
                        "Should have exactly 1 valid chunk after validation");
            
        } finally {
            merkleState.close();
            if (merkleRef instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) merkleRef).close();
                } catch (Exception e) {
                    // Ignore close errors in test cleanup
                }
            }
        }
    }

    @Test
    public void testCreateStateFromRef_SameAsFromRefMethod() throws IOException, ExecutionException, InterruptedException {
        // Create test data
        Path testFile = tempDir.resolve("comparison_data.bin");
        byte[] data = new byte[1024 * 1024]; // 1MB file
        Random random = new Random(54321);
        random.nextBytes(data);
        Files.write(testFile, data);

        // Create MerkleRef from the test data
        MerkleRef merkleRef = MerkleRefFactory.fromDataSimple(testFile).get();
        
        // Create MerkleState using the new factory method
        Path statePath1 = tempDir.resolve("state1.mrkl");
        MerkleState state1 = MerkleDataImpl.createStateFromRef(merkleRef, statePath1);
        
        // Create MerkleState using the interface method (which should delegate to the new method)
        Path statePath2 = tempDir.resolve("state2.mrkl");
        MerkleState state2 = MerkleState.fromRef(merkleRef, statePath2);

        try {
            // Both should have identical behavior
            assertEquals(state1.getMerkleShape().getLeafCount(), 
                        state2.getMerkleShape().getLeafCount(),
                        "Both methods should create states with same leaf count");
            
            assertEquals(state1.getValidChunks().cardinality(), 
                        state2.getValidChunks().cardinality(),
                        "Both methods should create states with same valid chunk count");
            
            assertEquals(0, state1.getValidChunks().cardinality(),
                        "Factory method should create state with no valid chunks");
            assertEquals(0, state2.getValidChunks().cardinality(),
                        "Interface method should create state with no valid chunks");
            
        } finally {
            state1.close();
            state2.close();
            if (merkleRef instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) merkleRef).close();
                } catch (Exception e) {
                    // Ignore close errors in test cleanup
                }
            }
        }
    }
}