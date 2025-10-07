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

import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Focused test for MAFileChannel state validation behavior.
 * This test verifies that chunks are marked as valid when downloaded and verified,
 * and that this state persists correctly.
 */
@ExtendWith(JettyFileServerExtension.class)
class MAFileChannelStateValidationTest {

    @Test
    void testChunkValidationAndStatePersistence(@TempDir Path tempDir) throws Exception {
        // Create a smaller test file with known data
        int fileSize = 3 * 1024 * 1024; // 3MB file (should give us 3 chunks with 1MB each)
        Path sourceFile = createTestFile(tempDir, "state_validation.bin", fileSize);
        
        // Serve the file via HTTP
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path serverDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("state_validation_" + uniqueId);
        Files.createDirectories(serverDir);
        Path serverFile = serverDir.resolve("state_validation.bin");
        Files.copy(sourceFile, serverFile);
        
        // Create merkle reference file
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);
        
        // Print shape information for debugging
        MerkleShape shape = merkleRef.getShape();
        System.out.println("=== Merkle Shape Information ===");
        System.out.println("Content size: " + shape.getTotalContentSize());
        System.out.println("Chunk size: " + shape.getChunkSize());
        System.out.println("Total chunks: " + shape.getLeafCount());
        System.out.println("Node count: " + shape.getNodeCount());
        
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String remoteUrl = baseUrl + "temp/state_validation_" + uniqueId + "/state_validation.bin";
        
        Path localCache = tempDir.resolve("cache.dat");
        Path merkleStatePath = tempDir.resolve("state.mrkl");
        
        // Phase 1: Download and verify specific chunks one by one
        System.out.println("\n=== Phase 1: Step-by-step chunk validation ===");
        try (MAFileChannel channel = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            // Test chunk 0
            System.out.println("\n--- Testing Chunk 0 ---");
            testSingleChunk(channel, merkleStatePath, 0, 0, 100);
            
            // Test chunk 1
            System.out.println("\n--- Testing Chunk 1 ---");
            long chunk1Position = shape.getChunkSize() + 100; // Read from middle of chunk 1
            testSingleChunk(channel, merkleStatePath, 1, chunk1Position, 100);
            
            // Test chunk 2
            System.out.println("\n--- Testing Chunk 2 ---");
            long chunk2Position = 2 * shape.getChunkSize() + 200; // Read from middle of chunk 2
            testSingleChunk(channel, merkleStatePath, 2, chunk2Position, 100);
            
            // Force flush
            channel.force(true);
        }
        
        // Phase 2: Verify persistence after closing and reopening
        System.out.println("\n=== Phase 2: State persistence verification ===");
        MerkleState persistedState = MerkleState.load(merkleStatePath);
        BitSet persistedValid = persistedState.getValidChunks();
        System.out.println("Persisted valid chunks: " + persistedValid);
        
        assertTrue(persistedValid.get(0), "Chunk 0 should be persisted as valid");
        assertTrue(persistedValid.get(1), "Chunk 1 should be persisted as valid");
        assertTrue(persistedValid.get(2), "Chunk 2 should be persisted as valid");
        assertEquals(3, persistedValid.cardinality(), "All 3 chunks should be valid");
        
        persistedState.close();
        
        // Phase 3: Reopen and verify no additional downloads needed
        System.out.println("\n=== Phase 3: No re-download verification ===");
        try (MAFileChannel channel2 = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            // Reading from any previously downloaded chunks should NOT trigger downloads
            ByteBuffer testBuffer = ByteBuffer.allocate(50);
            Future<Integer> readFuture = channel2.read(testBuffer, shape.getChunkSize() + 50);
            int bytesRead = readFuture.get();
            assertEquals(50, bytesRead);
            
            // Verify state is unchanged (no new chunks should be marked valid since they were already valid)
            MerkleState finalState = MerkleState.load(merkleStatePath);
            BitSet finalValid = finalState.getValidChunks();
            assertEquals(persistedValid, finalValid, "Valid chunks should remain unchanged");
            finalState.close();
        }
        
        // Cleanup
        Files.deleteIfExists(serverFile);
        Files.deleteIfExists(mrefPath);
        merkleRef.close();
    }
    
    private void testSingleChunk(MAFileChannel channel, Path merkleStatePath, int expectedChunkIndex, 
                                long readPosition, int readSize) throws Exception {
        
        // Check initial state
        MerkleState stateBefore = MerkleState.load(merkleStatePath);
        boolean validBefore = stateBefore.isValid(expectedChunkIndex);
        System.out.println("Chunk " + expectedChunkIndex + " valid before read: " + validBefore);
        stateBefore.close();
        
        // Perform read
        ByteBuffer buffer = ByteBuffer.allocate(readSize);
        Future<Integer> readFuture = channel.read(buffer, readPosition);
        int bytesRead = readFuture.get();
        System.out.println("Read " + bytesRead + " bytes from position " + readPosition);
        assertEquals(readSize, bytesRead);
        
        // Force state to be written
        channel.force(true);
        Thread.sleep(50); // Small delay to ensure async operations complete
        
        // Check state after read
        MerkleState stateAfter = MerkleState.load(merkleStatePath);
        boolean validAfter = stateAfter.isValid(expectedChunkIndex);
        System.out.println("Chunk " + expectedChunkIndex + " valid after read: " + validAfter);
        
        if (!validBefore && !validAfter) {
            // The chunk should now be valid since we just downloaded it
            fail("Chunk " + expectedChunkIndex + " should be marked as valid after download, but it's not. " +
                 "This indicates the verification/validation process is not working correctly.");
        }
        
        assertTrue(validAfter, "Chunk " + expectedChunkIndex + " must be valid after being read and verified");
        stateAfter.close();
    }
    
    @Test
    void testMerkleStateDirectValidation(@TempDir Path tempDir) throws Exception {
        // Test MerkleState validation directly, without MAFileChannel
        System.out.println("\n=== Direct MerkleState Validation Test ===");
        
        // Create test data
        Path sourceFile = createTestFile(tempDir, "direct_validation.bin", 2 * 1024 * 1024);
        
        // Create merkle reference
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(sourceFile).get();
        Path mrefPath = sourceFile.resolveSibling(sourceFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);
        
        // Create state from reference
        Path statePath = tempDir.resolve("direct_state.mrkl");
        MerkleState state = MerkleState.fromRef(merkleRef, statePath);
        
        MerkleShape shape = state.getMerkleShape();
        System.out.println("Shape - chunks: " + shape.getLeafCount() + ", chunk size: " + shape.getChunkSize());
        
        // Initially no chunks should be valid
        BitSet initialValid = state.getValidChunks();
        assertEquals(0, initialValid.cardinality(), "Initially no chunks should be valid");
        
        // Read actual chunk data from source file and validate it
        for (int chunkIndex = 0; chunkIndex < shape.getLeafCount(); chunkIndex++) {
            long chunkStart = chunkIndex * shape.getChunkSize();
            int chunkSize = (int) Math.min(shape.getChunkSize(), Files.size(sourceFile) - chunkStart);
            
            byte[] chunkData = new byte[chunkSize];
            try (var fc = java.nio.channels.FileChannel.open(sourceFile, java.nio.file.StandardOpenOption.READ)) {
                ByteBuffer buffer = ByteBuffer.wrap(chunkData);
                fc.read(buffer, chunkStart);
            }
            
            // Validate the chunk
            final int index = chunkIndex;
            boolean saved = state.saveIfValid(chunkIndex, ByteBuffer.wrap(chunkData), data -> {
                System.out.println("Chunk " + index + " validated and saved");
            });
            
            assertTrue(saved, "Chunk " + chunkIndex + " should be valid (actual file data)");
            assertTrue(state.isValid(chunkIndex), "Chunk " + chunkIndex + " should be marked as valid");
        }
        
        // Verify all chunks are now valid
        BitSet finalValid = state.getValidChunks();
        assertEquals(shape.getLeafCount(), finalValid.cardinality(), "All chunks should be valid");
        
        state.close();
        merkleRef.close();
    }
    
    private Path createTestFile(Path dir, String name, int size) throws IOException {
        Path file = dir.resolve(name);
        byte[] data = new byte[size];
        Random random = new Random(54321); // Different seed for different test content
        random.nextBytes(data);
        Files.write(file, data);
        return file;
    }
}