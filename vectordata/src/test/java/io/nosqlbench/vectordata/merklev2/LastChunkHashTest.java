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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class LastChunkHashTest {

    @TempDir
    Path tempDir;
    
    private Path testDataFile;

    @BeforeEach
    void setUp() throws IOException {
        testDataFile = tempDir.resolve("test_data.bin");
        
        // Create test data file where the last chunk is smaller than chunk size
        // Use 1.5MB (1.5 * 1024 * 1024 = 1572864 bytes) with default 1MB chunks
        // This creates 2 chunks: chunk 0 (1MB) and chunk 1 (0.5MB)
        byte[] testData = new byte[1572864]; 
        new Random(42).nextBytes(testData);
        Files.write(testDataFile, testData);
    }

    @Test
    void testLastChunkValidation() throws Exception {
        // Create merkle reference from the test data
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(testDataFile);
        MerkleDataImpl merkleRef = progress.getFuture().get();
        
        // Verify we have the expected structure
        MerkleShape shape = merkleRef.getShape();
        assertEquals(2, shape.getTotalChunks(), "Should have 2 chunks");
        assertEquals(1048576, shape.getChunkSize(), "Chunk size should be 1MB");
        
        // Check chunk sizes
        long chunk0Size = shape.getChunkEndPosition(0) - shape.getChunkStartPosition(0);
        long chunk1Size = shape.getChunkEndPosition(1) - shape.getChunkStartPosition(1);
        
        assertEquals(1048576, chunk0Size, "First chunk should be 1MB");
        assertEquals(524288, chunk1Size, "Last chunk should be 0.5MB");
        
        // Create a MerkleState to test validation
        Path stateFile = tempDir.resolve("test.mrkl");
        MerkleState state = MerkleState.fromRef(merkleRef, stateFile);
        
        try {
            // Read the actual data chunks and test validation
            byte[] fileData = Files.readAllBytes(testDataFile);
            
            // Test chunk 0 validation (full chunk)
            ByteBuffer chunk0Data = ByteBuffer.wrap(fileData, 0, (int) chunk0Size);
            boolean chunk0Valid = state.saveIfValid(0, chunk0Data, data -> {});
            assertTrue(chunk0Valid, "Chunk 0 should validate correctly");
            
            // Test chunk 1 validation (partial chunk - this was failing before the fix)
            ByteBuffer chunk1Data = ByteBuffer.wrap(fileData, (int) chunk0Size, (int) chunk1Size);
            boolean chunk1Valid = state.saveIfValid(1, chunk1Data, data -> {});
            assertTrue(chunk1Valid, "Chunk 1 (last chunk) should validate correctly");
            
            // Also test with a buffer that has extra capacity (simulating the download scenario)
            ByteBuffer oversizedBuffer = ByteBuffer.allocate(1048576); // Full chunk size buffer
            oversizedBuffer.put(fileData, (int) chunk0Size, (int) chunk1Size); // Only put the actual chunk data
            oversizedBuffer.flip();
            
            boolean chunk1ValidOversized = state.saveIfValid(1, oversizedBuffer, data -> {});
            assertTrue(chunk1ValidOversized, "Chunk 1 should validate correctly even with oversized buffer");
            
        } finally {
            state.close();
            merkleRef.close();
        }
    }
    
    @Test
    void testCorruptedLastChunkDetection() throws Exception {
        // Create merkle reference from the test data
        MerkleRefBuildProgress progress = MerkleRefFactory.fromData(testDataFile);
        MerkleDataImpl merkleRef = progress.getFuture().get();
        
        // Create a MerkleState to test validation
        Path stateFile = tempDir.resolve("test.mrkl");
        MerkleState state = MerkleState.fromRef(merkleRef, stateFile);
        
        try {
            // Read the actual data and corrupt the last chunk
            byte[] fileData = Files.readAllBytes(testDataFile);
            MerkleShape shape = merkleRef.getShape();
            long chunk0Size = shape.getChunkEndPosition(0) - shape.getChunkStartPosition(0);
            long chunk1Size = shape.getChunkEndPosition(1) - shape.getChunkStartPosition(1);
            
            // Corrupt the last chunk by changing one byte
            byte[] corruptedData = fileData.clone();
            corruptedData[corruptedData.length - 1] ^= 0xFF; // Flip all bits in last byte
            
            ByteBuffer corruptedChunk1 = ByteBuffer.wrap(corruptedData, (int) chunk0Size, (int) chunk1Size);
            boolean chunk1Valid = state.saveIfValid(1, corruptedChunk1, data -> {});
            assertFalse(chunk1Valid, "Corrupted last chunk should fail validation");
            
        } finally {
            state.close();
            merkleRef.close();
        }
    }
}