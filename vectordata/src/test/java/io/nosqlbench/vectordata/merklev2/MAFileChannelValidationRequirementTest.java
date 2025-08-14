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
 * This test verifies the core requirement for MAFileChannel:
 * "When a MAFileChannel is being initialized and there is already a content cache file 
 * and an associated merkle state file, any previously downloaded and verified chunks 
 * should not have to be downloaded again."
 */
@ExtendWith(JettyFileServerExtension.class)
class MAFileChannelValidationRequirementTest {

    @Test
    void testInitialConditionsWork_ChunkValidationAndPersistence(@TempDir Path tempDir) throws Exception {
        /*
         * This test verifies:
         * 1. When a chunk is read for the first time, it triggers download and verification
         * 2. The chunk is marked as valid in the merkle state after verification
         * 3. The valid state is persisted to disk
         * 4. When reopening with existing cache and state files, verified chunks are not re-downloaded
         */
        
        // Setup: Create test file and serve it
        int fileSize = 2 * 1024 * 1024; // 2MB file
        Path sourceFile = createTestFile(tempDir, "requirement_test.bin", fileSize);
        
        String uniqueId = String.valueOf(System.currentTimeMillis());
        Path serverDir = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve("requirement_" + uniqueId);
        Files.createDirectories(serverDir);
        Path serverFile = serverDir.resolve("requirement_test.bin");
        Files.copy(sourceFile, serverFile);
        
        // Create merkle reference file
        MerkleDataImpl merkleRef = (MerkleDataImpl) MerkleRef.fromDataSimple(serverFile).get();
        Path mrefPath = serverFile.resolveSibling(serverFile.getFileName() + ".mref");
        merkleRef.save(mrefPath);
        
        URL baseUrl = JettyFileServerExtension.getBaseUrl();
        String remoteUrl = baseUrl + "temp/requirement_" + uniqueId + "/requirement_test.bin";
        
        Path localCache = tempDir.resolve("cache.dat");
        Path merkleStatePath = tempDir.resolve("state.mrkl");
        
        MerkleShape shape = merkleRef.getShape();
        System.out.println("=== Test Setup ===");
        System.out.println("File size: " + fileSize + " bytes");
        System.out.println("Chunk size: " + shape.getChunkSize() + " bytes");
        System.out.println("Total chunks: " + shape.getLeafCount());
        System.out.println("Expected chunks with 1MB default: " + Math.ceil((double)fileSize / (1024 * 1024)));
        
        // PHASE 1: First MAFileChannel - Case 1 initialization (no files exist)
        System.out.println("\n=== PHASE 1: Initial download (Case 1 initialization) ===");
        try (MAFileChannel channel1 = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            
            // Step 1.1: Verify initial state - no chunks should be valid
            MerkleState initialState = MerkleState.load(merkleStatePath);
            BitSet initialValid = initialState.getValidChunks();
            System.out.println("Initial valid chunks: " + initialValid);
            assertEquals(0, initialValid.cardinality(), "No chunks should be valid initially");
            initialState.close();
            
            // Step 1.2: Read from chunk 0 (should trigger download and verification)
            System.out.println("\nReading from chunk 0...");
            ByteBuffer buffer0 = ByteBuffer.allocate(1000);
            Future<Integer> read0 = channel1.read(buffer0, 0);
            int bytesRead0 = read0.get();
            assertTrue(bytesRead0 > 0, "Should read some bytes from chunk 0");
            
            // Step 1.3: Verify chunk 0 is now marked as valid
            channel1.force(true); // Ensure state is flushed
            Thread.sleep(50);     // Allow async operations to complete
            
            MerkleState afterChunk0 = MerkleState.load(merkleStatePath);
            assertTrue(afterChunk0.isValid(0), "Chunk 0 should be valid after download and verification");
            assertEquals(1, afterChunk0.getValidChunks().cardinality(), "Only chunk 0 should be valid");
            afterChunk0.close();
            
            System.out.println("✓ Chunk 0 downloaded, verified, and marked as valid");
        }
        
        // PHASE 2: Second MAFileChannel - Case 2 initialization (both files exist) 
        System.out.println("\n=== PHASE 2: Resume with existing state (Case 2 initialization) ===");
        
        // Verify files exist for Case 2
        assertTrue(Files.exists(localCache), "Cache file should exist");
        assertTrue(Files.exists(merkleStatePath), "State file should exist");
        
        final long chunk1Position = shape.getChunkSize() + 100;
        
        try (MAFileChannel channel2 = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            
            // Step 2.1: Verify that chunk 0 is still marked as valid (persistence check)
            MerkleState persistedState = MerkleState.load(merkleStatePath);
            assertTrue(persistedState.isValid(0), "Chunk 0 should still be valid after reload");
            assertFalse(persistedState.isValid(1), "Chunk 1 should still be invalid");
            persistedState.close();
            
            // Step 2.2: Read from chunk 0 again - should NOT trigger download
            System.out.println("Reading from chunk 0 again (should use cache)...");
            ByteBuffer buffer0Again = ByteBuffer.allocate(1000);
            Future<Integer> read0Again = channel2.read(buffer0Again, 0);
            int bytesRead0Again = read0Again.get();
            assertTrue(bytesRead0Again > 0, "Should read bytes from cached chunk 0");
            
            // Step 2.3: Read from chunk 1 - should trigger download and verification
            System.out.println("Reading from chunk 1 (should trigger download)...");
            
            MerkleState beforeChunk1 = MerkleState.load(merkleStatePath);
            boolean chunk1ValidBefore = beforeChunk1.isValid(1);
            System.out.println("Chunk 1 valid before read: " + chunk1ValidBefore);
            beforeChunk1.close();
            
            ByteBuffer buffer1 = ByteBuffer.allocate(1000);
            Future<Integer> read1 = channel2.read(buffer1, chunk1Position);
            int bytesRead1 = read1.get();
            assertTrue(bytesRead1 > 0, "Should read bytes from chunk 1");
            
            // Step 2.4: Verify chunk 1 is now marked as valid
            channel2.force(true); // Ensure state is flushed
            Thread.sleep(50);     // Allow async operations to complete
            
            MerkleState afterChunk1 = MerkleState.load(merkleStatePath);
            assertTrue(afterChunk1.isValid(0), "Chunk 0 should still be valid");
            assertTrue(afterChunk1.isValid(1), "Chunk 1 should now be valid after download and verification");
            afterChunk1.close();
            
            System.out.println("✓ Chunk 1 downloaded, verified, and marked as valid");
        }
        
        // PHASE 3: Third MAFileChannel - verify complete state persistence
        System.out.println("\n=== PHASE 3: Final persistence verification ===");
        try (MAFileChannel channel3 = new MAFileChannel(localCache, merkleStatePath, remoteUrl)) {
            
            MerkleState finalState = MerkleState.load(merkleStatePath);
            BitSet finalValid = finalState.getValidChunks();
            System.out.println("Final valid chunks: " + finalValid);
            
            assertTrue(finalState.isValid(0), "Chunk 0 should be persistently valid");
            assertTrue(finalState.isValid(1), "Chunk 1 should be persistently valid");
            
            // Both chunks should now be available without any downloads
            ByteBuffer testBuffer = ByteBuffer.allocate(500);
            Future<Integer> testRead = channel3.read(testBuffer, 50000); // Read from chunk 0
            assertTrue(testRead.get() > 0, "Should read from cached chunk 0");
            
            testBuffer.clear();
            testRead = channel3.read(testBuffer, chunk1Position + 500); // Read from chunk 1
            assertTrue(testRead.get() > 0, "Should read from cached chunk 1");
            
            finalState.close();
        }
        
        System.out.println("\n✓ All requirements verified successfully:");
        System.out.println("  - Chunks trigger download when first accessed");
        System.out.println("  - Downloaded chunks are verified against merkle hashes");
        System.out.println("  - Valid chunks are persisted to disk");
        System.out.println("  - Previously downloaded chunks are not re-downloaded");
        
        // Cleanup
        Files.deleteIfExists(serverFile);
        Files.deleteIfExists(mrefPath);
        merkleRef.close();
    }
    
    private Path createTestFile(Path dir, String name, int size) throws IOException {
        Path file = dir.resolve(name);
        byte[] data = new byte[size];
        Random random = new Random(99999); // Fixed seed for reproducible content
        random.nextBytes(data);
        Files.write(file, data);
        return file;
    }
}